use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
};

use crate::message::{Body, BodyWithMsgId, Message};

type NodeOrClientName = String;

pub struct MessageWithResponder {
    msg: Message,
    responder: Option<tokio::sync::oneshot::Sender<Message>>,
}

pub struct Node {
    pub my_id: OnceLock<String>,
    pub other_node_ids: OnceLock<Vec<String>>,
    pub unacked: Arc<Mutex<HashMap<usize, tokio::sync::oneshot::Sender<Message>>>>,
    pub stdout_tx: OnceLock<tokio::sync::mpsc::Sender<MessageWithResponder>>,
    pub next_msg_id: AtomicUsize,
}

impl Node {
    pub fn new() -> Self {
        Self {
            unacked: Default::default(),
            stdout_tx: OnceLock::new(),
            next_msg_id: AtomicUsize::new(0),
            my_id: OnceLock::new(),
            other_node_ids: OnceLock::new(),
        }
    }

    pub async fn send(
        self: Arc<Self>,
        dest: NodeOrClientName,
        body: Body,
        responder: Option<tokio::sync::oneshot::Sender<Message>>,
    ) -> Message {
        let stdout_tx = self.stdout_tx.get().unwrap();

        let msg = Message {
            src: self.my_id.get().unwrap().into(),
            dest,
            body: BodyWithMsgId {
                msg_id: self.reserve_next_msg_id(),
                inner: body,
            },
        };

        stdout_tx
            .send(MessageWithResponder {
                msg: msg.clone(),
                responder,
            })
            .await
            .unwrap();
        msg
    }

    pub async fn broadcast(
        self: Arc<Self>,
        body: Body,
        responder: Option<tokio::sync::mpsc::Sender<Message>>,
    ) {
        let mut receiver_tasks = tokio::task::JoinSet::<Message>::new();

        for destination in self.other_node_ids.get().unwrap().clone() {
            let (tx, rx) = tokio::sync::oneshot::channel::<Message>();
            receiver_tasks.spawn(async move {
                rx.await
                    .expect("should be able to recv on one of the broadcast responses")
            });

            self.clone().send(destination, body.clone(), Some(tx)).await;
        }

        tokio::spawn(async move {
            while let Some(response_result) = receiver_tasks.join_next().await {
                let response_message =
                    response_result.expect("should be able to recv response during broadcast");
                if let Some(ref responder) = responder {
                    // The caller of broadcast might not care for all responses.
                    // In such case, some receivers might be dropped already
                    if !responder.is_closed() {
                        // There is still chance for a receiver to be dropped
                        // after the is_closed() check, so ignore the result
                        let _ = responder.send(response_message).await;
                    }
                }
            }
        });
    }

    pub async fn run(self: Arc<Self>) -> tokio::sync::mpsc::Receiver<Message> {
        let mut stdin_rx = self.clone().spawn_stdin_task().await;
        self.clone().spawn_stdout_task().await;

        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            while let Some(msg) = stdin_rx.recv().await {
                let mut responder: Option<tokio::sync::oneshot::Sender<Message>> = None;
                {
                    let mut unacked = self.unacked.lock().unwrap();
                    responder = unacked.remove(&msg.body.msg_id);
                }

                if let Some(responder) = responder {
                    responder.send(msg).unwrap();
                } else {
                    tx.send(msg).await.unwrap();
                }
            }
        });

        rx
    }

    pub async fn spawn_stdout_task(self: Arc<Self>) {
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
                        .insert(msg.body.msg_id, responder);
                }
            }
        });
    }

    pub async fn spawn_stdin_task(self: Arc<Self>) -> tokio::sync::mpsc::Receiver<Message> {
        let (stdin_tx, stdin_rx) = tokio::sync::mpsc::channel::<Message>(32);
        tokio::spawn(async move {
            let mut input = String::new();
            let mut is_reading_stdin = true;
            while is_reading_stdin {
                if let Err(e) = std::io::stdin().read_line(&mut input) {
                    println!("readline error: {e}");
                    is_reading_stdin = false;
                }

                let json_msg: Message = serde_json::from_str(&input)
                    .expect(&format!("should take a JSON message. Got {:?}", input));
                tracing::debug!("received json msg: {:?}", json_msg);

                if let Body::Init {
                    node_id, node_ids, ..
                } = &json_msg.body.inner
                {
                    self.my_id.set(node_id.into());

                    self.other_node_ids.set(
                        node_ids
                            .iter()
                            .filter(|id| *id != node_id)
                            .cloned()
                            .collect(),
                    );
                }

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
