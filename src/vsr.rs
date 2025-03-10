use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crate::{
    kv_store::KeyValueStore,
    message::{Body, ErrorCode, Message},
    node::Node,
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

pub type StateMachineKey = usize;
pub type StateMachineValue = usize;

pub struct VSR {
    op_log: Mutex<Vec<Message>>,
    op_number: AtomicUsize,
    commit_number: AtomicUsize,
    view_number: AtomicUsize,
    primary_node: Mutex<NodeName>,
    status: Mutex<NodeStatus>,
    configuration: Mutex<Vec<NodeName>>, // All nodes in the cluster
    replica_number: AtomicUsize,
    client_table: Mutex<HashMap<ClientName, ClientTableEntry>>,
    node: Arc<Node>,
    state_machine: Mutex<KeyValueStore<StateMachineKey, StateMachineValue>>,
}

impl VSR {
    pub fn new() -> Self {
        VSR {
            op_log: Mutex::new(Vec::new()),
            op_number: AtomicUsize::new(0),
            commit_number: AtomicUsize::new(0),
            view_number: AtomicUsize::new(0),
            primary_node: Default::default(),
            status: Mutex::new(NodeStatus::Normal),
            configuration: Default::default(),
            replica_number: AtomicUsize::new(0),
            client_table: Default::default(),
            node: Arc::new(Node::new()),
            state_machine: Default::default(),
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut stdin_rx = self.node.clone().spawn_stdin_task().await;
        self.node.clone().spawn_stdout_task().await;

        loop {
            tokio::select! {
                Some(msg) = stdin_rx.recv() => {
                    let mut responder: Option<tokio::sync::oneshot::Sender<Message>> = None;
                    {
                        let mut unacked = self.node.unacked.lock().unwrap();
                        responder = unacked.remove(&msg.body.msg_id);
                    }

                    if let Some(responder) = responder {
                        responder.send(msg).unwrap();
                    } else {
                        tokio::spawn({
                            let vsr = self.clone();
                            async move { vsr.handle(msg).await }
                        });
                    }
                }
            };
        }
    }

    async fn handle(self: Arc<Self>, msg: Message) -> anyhow::Result<()> {
        match msg.body.inner {
            Body::Init { node_id, node_ids } => {
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

                let _ = self
                    .node
                    .clone()
                    .send(
                        msg.src,
                        Body::InitOk {
                            in_reply_to: msg.body.msg_id,
                        },
                        None,
                    )
                    .await;
            }
            Body::Write { .. } | Body::Read { .. } | Body::Cas { .. } => {
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
                        let _ = self
                            .node
                            .clone()
                            .send(msg.src, cached_response.body.inner, None)
                            .await;
                    }
                    Some(ClientTableEntry { op, .. }) if msg.body.msg_id < op.body.msg_id => {
                        let error_text =
                            String::from("Normal Protocol: received a stale request number");
                        let _ = self
                            .node
                            .clone()
                            .send(
                                msg.src,
                                Body::Error {
                                    in_reply_to: msg.body.msg_id,
                                    code: crate::message::ErrorCode::Abort,
                                    text: error_text,
                                },
                                None,
                            )
                            .await;
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
            view_number: self.view_number.load(Ordering::SeqCst),
            op: Box::new(msg.clone()),
            op_number: self.op_number.load(Ordering::SeqCst),
            commit_number: self.commit_number.load(Ordering::SeqCst),
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(replica_count);
        self.node.clone().broadcast(body, Some(tx)).await;

        // We count ourselves as part of the consensus majority
        let mut remaining_response_count = self.clone().majority_count() - 1;

        while let Some(response) = rx.recv().await {
            tracing::debug!("remaining_response_count: {remaining_response_count}");
            match response.body.inner {
                Body::PrepareOk { .. } => {
                    remaining_response_count -= 1;

                    // TODO - check if this is needed ( step 5, normal operation )
                    // Then, after it has executed all earlier operations
                    // (those assigned smaller op-numbers)

                    if remaining_response_count == 0 {
                        break;
                    }
                }
                _ => panic!(
                    "expected a PrepareOk response to Prepare broadcast msg. Got: {:?}",
                    response
                ),
            }
        }

        self.commit_number.fetch_add(1, Ordering::SeqCst);

        let mut my_response: Option<Message> = None;

        match msg.body.inner {
            Body::Read { key } => {
                let result = self.state_machine.lock().unwrap().read(&key).cloned();

                let body = match result {
                    Some(value) => Body::ReadOk {
                        in_reply_to: msg.body.msg_id,
                        value,
                    },
                    None => {
                        let err = ErrorCode::KeyDoesNotExist;
                        Body::Error {
                            in_reply_to: msg.body.msg_id,
                            code: err.clone(),
                            text: err.to_string(),
                        }
                    }
                };

                my_response = Some(self.node.clone().send(msg.src.clone(), body, None).await);
            }
            Body::Write { key, value } => {
                self.clone().state_machine.lock().unwrap().write(key, value);
                my_response = Some(
                    self.node
                        .clone()
                        .send(
                            msg.src.clone(),
                            Body::WriteOk {
                                in_reply_to: msg.body.msg_id,
                            },
                            None,
                        )
                        .await,
                );
            }
            Body::Cas { key, from, to } => {
                let result = self
                    .clone()
                    .state_machine
                    .lock()
                    .unwrap()
                    .cas(key, from, to);

                let body = match result {
                    Ok(()) => Body::CasOk {
                        in_reply_to: msg.body.msg_id,
                    },
                    Err(e) => match e.downcast_ref::<ErrorCode>() {
                        Some(e @ ErrorCode::PreconditionFailed)
                        | Some(e @ ErrorCode::KeyDoesNotExist) => Body::Error {
                            in_reply_to: msg.body.msg_id,
                            code: e.clone(),
                            text: e.to_string(),
                        },
                        _ => panic!("encountered an unexpected error while processing Cas request"),
                    },
                };

                my_response = Some(self.node.clone().send(msg.src.clone(), body, None).await);
            }
            _ => unreachable!(),
        }

        self.client_table.lock().unwrap().insert(
            msg.src.clone(),
            ClientTableEntry {
                op: msg,
                response: my_response,
            },
        );
    }

    fn majority_count(self: Arc<Self>) -> usize {
        let all_nodes_count = self.node.other_node_ids.get().unwrap().len() + 1;
        (all_nodes_count / 2) + 1
    }
}
