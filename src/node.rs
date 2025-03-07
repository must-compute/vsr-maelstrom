use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
};

use crate::message::{Body, Message};

type NodeName = String;
type ClientName = String;
type NodeOrClientName = String;

struct MessageWithResponder {
    msg: Message,
    responder: Option<tokio::sync::oneshot::Sender<Message>>,
}

#[derive(Debug, Clone)]
struct ClientTableEntry {
    op: Message,
    response: Option<Message>, // None if still processing
}

enum NodeStatus {
    Normal,
    ViewChange,
    Recovering,
}

pub struct Node {
    unacked: Mutex<HashMap<usize, tokio::sync::oneshot::Sender<Message>>>,
    stdout_tx: OnceLock<tokio::sync::mpsc::Sender<MessageWithResponder>>,
    next_msg_id: AtomicUsize,

    // vsr
    op_log: Mutex<Vec<Message>>,
    op_number: AtomicUsize,
    commit_number: AtomicUsize,
    view_number: AtomicUsize,
    primary_node: Mutex<NodeName>,
    status: Mutex<NodeStatus>,
    configuration: Mutex<Vec<NodeName>>, // All nodes in the cluster
    replica_number: AtomicUsize,
    client_table: Mutex<HashMap<ClientName, ClientTableEntry>>,
}

impl Node {
    pub fn new() -> Self {
        Node {
            unacked: Default::default(),
            stdout_tx: OnceLock::new(),
            next_msg_id: AtomicUsize::new(0),

            op_log: Mutex::new(Vec::new()),
            op_number: AtomicUsize::new(0),
            commit_number: AtomicUsize::new(0),
            view_number: AtomicUsize::new(0),
            primary_node: Default::default(),
            status: Mutex::new(NodeStatus::Normal),
            configuration: Default::default(),
            replica_number: AtomicUsize::new(0),
            client_table: Default::default(),
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut stdin_rx = self.clone().spawn_stdin_task().await;
        self.clone().spawn_stdout_task().await;

        loop {
            tokio::select! {
                Some(msg) = stdin_rx.recv() => {
                    if let Some(responder) = self.unacked.lock().unwrap().remove(&msg.body.msg_id()) {
                        responder.send(msg).unwrap();
                    } else {
                        tokio::spawn({
                            let node = self.clone();
                            async move {node.handle(msg).await}
                        });
                    }
                }
            }
        }
    }

    async fn handle(self: Arc<Self>, msg: Message) -> anyhow::Result<()> {
        match msg.body {
            Body::Init {
                msg_id: in_reply_to,
                node_id,
                node_ids,
            } => {
                {
                    let mut configuration_guard = self.configuration.lock().unwrap();
                    for node_id in node_ids {
                        configuration_guard.push(node_id);
                    }

                    // all nodes must use the same order (view change happens round-robin)
                    configuration_guard.sort();

                    let my_replica = configuration_guard
                        .iter()
                        .position(|node_name| *node_name == node_id)
                        .unwrap();

                    self.replica_number.store(my_replica, Ordering::SeqCst);
                    *self.primary_node.lock().unwrap() =
                        configuration_guard.first().unwrap().clone();
                }

                let msg_id = self.reserve_next_msg_id();
                self.send(
                    msg.src,
                    Body::InitOk {
                        msg_id,
                        in_reply_to,
                    },
                    None,
                )
                .await
            }
            Body::Write { msg_id, .. } | Body::Read { msg_id, .. } | Body::Cas { msg_id, .. } => {
                let mut response = None;
                {
                    let client_table_guard = self.client_table.lock().unwrap();
                    response = client_table_guard.get(&msg.src).cloned();
                }

                match response {
                    Some(ClientTableEntry {
                        op,
                        response: Some(cached_response),
                    }) if msg == op => self.send(msg.src, cached_response.body, None).await,
                    Some(ClientTableEntry { op, .. }) if msg.body.msg_id() < op.body.msg_id() => {
                        let error_text =
                            String::from("Normal Protocol: received a stale request number");
                        self.send(
                            msg.src,
                            Body::Error {
                                in_reply_to: msg.body.msg_id(),
                                code: crate::message::ErrorCode::Abort,
                                text: error_text,
                            },
                            None,
                        )
                        .await
                    }
                    // request is valid and is not cached in the client table
                    _ => self.handle_client_request(msg).await,
                }
            }
            _ => {
                todo!()
            }
        };
        Ok(())
    }

    async fn handle_client_request(self: Arc<Self>, msg: Message) {
        // advance op-number
        // add the request at the end of the log
        // update the info for this client in the client table to contain the new request number s
        // broadcast (Prepare v m n k) to other replicas.
        //                    v: view number
        //                    m: msg from client
        //                    n: op number assigned to this request
        //                    k: commit number
        //
    }

    async fn broadcast(
        self: Arc<Self>,
        body: Body,
        responder: Option<tokio::sync::mpsc::Sender<Message>>,
    ) {
        let mut receiver_tasks = tokio::task::JoinSet::<Message>::new();

        let mut other_node_ids = vec![];
        {
            let configuration_guard = self.configuration.lock().unwrap();

            let my_id = configuration_guard
                .get(self.replica_number.load(Ordering::SeqCst))
                .unwrap();

            other_node_ids = configuration_guard
                .iter()
                .filter(|id| *id != my_id)
                .cloned()
                .collect();
        }

        for destination in other_node_ids {
            let (tx, rx) = tokio::sync::oneshot::channel::<Message>();
            receiver_tasks.spawn(async move {
                rx.await
                    .expect("should be able to recv on one of the broadcast responses")
            });

            let new_msg_id = self.clone().reserve_next_msg_id();

            let mut body = body.clone();
            body.set_msg_id(new_msg_id);
            self.clone().send(destination, body, Some(tx)).await;
        }

        tokio::spawn(async move {
            while let Some(response_result) = receiver_tasks.join_next().await {
                let response_message =
                    response_result.expect("should be able to recv response during broadcast");
                if let Some(ref responder) = responder {
                    responder
                        .send(response_message)
                        .await
                        .expect("should be able to return response message from broadcast()");
                }
            }
        });
    }

    async fn send(
        self: Arc<Self>,
        dest: NodeOrClientName,
        body: Body,
        responder: Option<tokio::sync::oneshot::Sender<Message>>,
    ) {
        let stdout_tx = self.stdout_tx.get().unwrap();

        let mut my_id = String::new();
        {
            let replica_number = self.replica_number.load(Ordering::SeqCst);
            let configuration = self.configuration.lock().unwrap();
            my_id = configuration.get(replica_number).unwrap().clone();
        }

        stdout_tx
            .send(MessageWithResponder {
                msg: Message {
                    src: my_id,
                    dest,
                    body,
                },
                responder,
            })
            .await
            .unwrap();
    }

    async fn spawn_stdout_task(self: Arc<Self>) {
        let (stdout_tx, mut stdout_rx) = tokio::sync::mpsc::channel::<MessageWithResponder>(32);

        self.stdout_tx.set(stdout_tx).unwrap();

        tokio::spawn(async move {
            while let Some(MessageWithResponder { msg, responder }) = stdout_rx.recv().await {
                println!(
                    "{}",
                    serde_json::to_string(&msg)
                        .expect("msg being sent to STDOUT should be serializable to JSON")
                );
                tracing::debug!("sent msg {:?}", &msg);

                if let Some(responder) = responder {
                    self.unacked
                        .lock()
                        .unwrap()
                        .insert(msg.body.msg_id(), responder);
                }
            }
        });
    }

    async fn spawn_stdin_task(self: Arc<Self>) -> tokio::sync::mpsc::Receiver<Message> {
        let (stdin_tx, stdin_rx) = tokio::sync::mpsc::channel::<Message>(32);
        tokio::spawn(async move {
            let mut input = String::new();
            let mut is_reading_stdin = true;
            while is_reading_stdin {
                if let Err(e) = std::io::stdin().read_line(&mut input) {
                    println!("readline error: {e}");
                    is_reading_stdin = false;
                }

                let json_msg = serde_json::from_str(&input)
                    .expect(&format!("should take a JSON message. Got {:?}", input));
                tracing::debug!("received json msg: {:?}", json_msg);

                stdin_tx.send(json_msg).await.unwrap();
                input.clear();
            }
        });
        stdin_rx
    }

    fn reserve_next_msg_id(&self) -> usize {
        self.next_msg_id.fetch_add(1, Ordering::SeqCst)
    }
}
