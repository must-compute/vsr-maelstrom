use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crate::{
    event::{Command, Event, Query},
    log::Log,
    message::{Body, Message},
};

type NodeName = String;
type ClientName = String;

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

pub async fn run() {
    let (event_tx, event_rx) = tokio::sync::mpsc::channel::<Event>(1000);
    let node = Arc::new(Node {
        event_tx,
        op_log: Mutex::new(Log::new()), // TODO should the log start empty instead?
        op_number: AtomicUsize::new(0),
        commit_number: AtomicUsize::new(0), // TODO is this a good default?
        view_number: AtomicUsize::new(0),
        primary_node: Mutex::new("n0".to_string()), // TODO obtain via init call
        status: Mutex::new(NodeStatus::Normal),
        configuration: Default::default(),
        replica_number: AtomicUsize::new(0),
        client_table: Default::default(),
    });

    node.run(event_rx).await;
}

struct Node {
    // channels. TODO init via lazy lock
    pub event_tx: tokio::sync::mpsc::Sender<Event>,

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
    async fn run(self: Arc<Self>, event_rx: tokio::sync::mpsc::Receiver<Event>) {
        let mut stdin_rx = self.clone().spawn_stdin_task().await;
        self.clone().spawn_event_handler_task(event_rx).await;

        loop {
            tokio::select! {
                Some(json_msg) = stdin_rx.recv() => {
                    tokio::spawn({
                        // let self = self.clone();
                        let node = self.clone();
                        async move {node.handle(json_msg).await}
                    });
                }
            }
        }
    }

    async fn handle(self: Arc<Self>, msg: Message) -> anyhow::Result<()> {
        match msg.body {
            Body::Init {
                msg_id,
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

                // TODO send init_ok
            }
            _ => {
                self.event_tx
                    .send(Event::Cast(Command::ReceivedViaMaelstrom { response: msg }))
                    .await?;
            }
        };
        Ok(())
    }

    async fn spawn_event_handler_task(
        self: Arc<Self>,
        mut event_rx: tokio::sync::mpsc::Receiver<Event>,
    ) {
        tokio::spawn(async move {
            let mut unacked: HashMap<usize, tokio::sync::oneshot::Sender<Message>> =
                Default::default();

            while let Some(event) = event_rx.recv().await {
                match event {
                    Event::Call(Query::SendViaMaelstrom { ref message, .. })
                    | Event::Cast(Command::SendViaMaelstrom { ref message }) => {
                        let message = message.clone();
                        println!(
                            "{}",
                            serde_json::to_string(&message)
                                .expect("msg being sent to STDOUT should be serializable to JSON")
                        );

                        tracing::debug!("sent msg {:?}", &message);

                        match event {
                            Event::Call(Query::SendViaMaelstrom { responder, .. }) => {
                                // TODO i dont like this at all, because     ^message
                                //      will have an old msg id if captured, due to shadowing
                                let msg_id = message.body.msg_id();
                                unacked.insert(msg_id, responder);
                                tracing::debug!("inserted to unacked with key: {msg_id}");
                            }
                            _ => (),
                        };
                    }
                    Event::Cast(Command::ReceivedViaMaelstrom { response }) => {
                        tracing::debug!(
                            "handling Event::Cast(Command::ReceivedViaMaelstrom {:?}",
                            &response
                        );
                        // we received an ack, so we notify and remove from unacked.
                        if let Some(notifier) = unacked.remove(&response.body.in_reply_to()) {
                            tracing::debug!(
                                "popped unacked in_reply_to: {:?}",
                                &response.body.in_reply_to()
                            );
                            notifier
                                .send(response)
                                .expect("returning msg ack should work over the oneshot channel");
                        }
                    }
                }
            }
        });
    }

    async fn spawn_stdin_task(self: Arc<Self>) -> tokio::sync::mpsc::Receiver<Message> {
        let (stdin_tx, stdin_rx) = tokio::sync::mpsc::channel::<Message>(32);
        // stdin task
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
}
