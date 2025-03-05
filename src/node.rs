use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crate::{
    log::Log,
    message::{Body, Message},
};

type NodeName = String;
type ClientName = String;
type NodeOrClientName = String;

struct MessageWithResponder {
    msg: Message,
    responder: Option<tokio::sync::oneshot::Sender<Message>>,
}

// TODO can i use msg_id from Message, instead of request number?
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
    stdout_tx: Mutex<Option<tokio::sync::mpsc::Sender<MessageWithResponder>>>,
    next_msg_id: AtomicUsize,

    // vsr
    op_log: Mutex<Log>,
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
            stdout_tx: Mutex::new(None),
            next_msg_id: AtomicUsize::new(0),

            op_log: Mutex::new(Log::new()), // TODO should the log start empty instead?
            op_number: AtomicUsize::new(0),
            commit_number: AtomicUsize::new(0), // TODO is this a good default?
            view_number: AtomicUsize::new(0),
            primary_node: Mutex::new("n0".to_string()), // TODO obtain via init call
            status: Mutex::new(NodeStatus::Normal),
            configuration: Default::default(),
            replica_number: AtomicUsize::new(0),
            client_table: Default::default(),
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut stdin_rx = self.clone().spawn_stdin_task().await;
        let stdout_tx = self.clone().spawn_stdout_task().await;
        *self.stdout_tx.lock().unwrap() = Some(stdout_tx);

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

                    let my_replica = configuration_guard
                        .iter()
                        .position(|node_name| *node_name == node_id)
                        .unwrap();

                    self.replica_number.store(my_replica, Ordering::SeqCst);
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
            _ => {
                todo!()
            }
        };
        Ok(())
    }

    async fn send(
        self: Arc<Self>,
        dest: NodeOrClientName,
        body: Body,
        responder: Option<tokio::sync::oneshot::Sender<Message>>,
    ) {
        let stdout_tx = self.stdout_tx.lock().unwrap().clone().unwrap();

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

    async fn spawn_stdout_task(self: Arc<Self>) -> tokio::sync::mpsc::Sender<MessageWithResponder> {
        let (stdout_tx, mut stdout_rx) = tokio::sync::mpsc::channel::<MessageWithResponder>(32);

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

        stdout_tx
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
