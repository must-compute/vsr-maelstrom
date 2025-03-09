use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crate::{
    message::{Body, Message},
    node2::Node2,
};

type NodeName = String;
type ClientName = String;

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
    node2: Arc<Node2>,
}

impl Node {
    pub fn new() -> Self {
        Node {
            op_log: Mutex::new(Vec::new()),
            op_number: AtomicUsize::new(0),
            commit_number: AtomicUsize::new(0),
            view_number: AtomicUsize::new(0),
            primary_node: Default::default(),
            status: Mutex::new(NodeStatus::Normal),
            configuration: Default::default(),
            replica_number: AtomicUsize::new(0),
            client_table: Default::default(),
            node2: Arc::new(Node2::new()),
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut rx = self.node2.clone().run().await;
        while let Some(msg) = rx.recv().await {
            tokio::spawn({
                let node = self.clone();
                async move { node.handle(msg).await }
            });
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

                let msg_id = self.node2.reserve_next_msg_id();
                self.node2
                    .clone()
                    .send(
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
                    }) if msg == op => {
                        self.node2
                            .clone()
                            .send(msg.src, cached_response.body, None)
                            .await
                    }
                    Some(ClientTableEntry { op, .. }) if msg.body.msg_id() < op.body.msg_id() => {
                        let error_text =
                            String::from("Normal Protocol: received a stale request number");
                        self.node2
                            .clone()
                            .send(
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
        self.op_number.fetch_add(1, Ordering::SeqCst);
        // add the request at the end of the log
        self.op_log.lock().unwrap().push(msg.clone());
        // update the info for this client in the client table to contain the new request number s
        self.client_table.lock().unwrap().insert(
            msg.src.clone(),
            ClientTableEntry {
                op: msg.clone(),
                response: None,
            },
        );
        // broadcast (Prepare v m n k) to other replicas.
        //                    v: view number
        //                    m: msg from client
        //                    n: op number assigned to this request
        //                    k: commit number
        let replica_count = self.configuration.lock().unwrap().len() - 1;
        let body = Body::Prepare {
            msg_id: 0, // will be filled by broadcast()
            view_number: self.view_number.load(Ordering::SeqCst),
            op: Box::new(msg.clone()),
            op_number: self.op_number.load(Ordering::SeqCst),
            commit_number: self.commit_number.load(Ordering::SeqCst),
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(replica_count);
        self.broadcast(body, Some(tx)).await;

        let mut remaining_response_count = replica_count;
        while let Some(response) = rx.recv().await {
            tracing::debug!("remaining_response_count: {remaining_response_count}");
            match response.body {
                Body::PrepareOk { .. } => {
                    todo!()
                }
                _ => panic!(
                    "expected a PrepareOk response to Prepare broadcast msg. Got: {:?}",
                    response
                ),
            }
        }
    }
}
