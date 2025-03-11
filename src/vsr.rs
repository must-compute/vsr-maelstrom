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
    //msg_id of a client request
    request_number: usize,
    response_body: Option<Body>, // None if still processing
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
        let mut rx = self.node.clone().run().await;

        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    tokio::spawn({
                        let vsr = self.clone();
                        async move { vsr.handle(msg).await }
                    });
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
            Body::Proxy { proxied_msg } => {
                let my_id = self.node.my_id.get().cloned().unwrap();
                let leader_id = self.clone().primary_node.lock().unwrap().clone();
                if my_id != leader_id {
                    self.node
                        .clone()
                        .send(leader_id, Body::Proxy { proxied_msg }, None)
                        .await;
                } else {
                    Box::pin(self.handle(*proxied_msg)).await?;
                }
            }
            Body::Write { .. } | Body::Read { .. } | Body::Cas { .. } => {
                let my_id = self.node.my_id.get().cloned().unwrap();
                let leader_id = self.clone().primary_node.lock().unwrap().clone();
                if my_id != leader_id {
                    self.node
                        .clone()
                        .send(
                            leader_id,
                            Body::Proxy {
                                proxied_msg: Box::new(msg),
                            },
                            None,
                        )
                        .await;
                    return Ok(());
                }

                let mut response = None;
                {
                    let client_table_guard = self.client_table.lock().unwrap();
                    response = client_table_guard.get(&msg.src).cloned();
                }

                match response {
                    Some(ClientTableEntry {
                        request_number,
                        response_body: Some(cached_response_body),
                    }) if msg.body.msg_id == request_number => {
                        let _ = self
                            .node
                            .clone()
                            .send(msg.src, cached_response_body, None)
                            .await;
                    }
                    Some(ClientTableEntry { request_number, .. })
                        if msg.body.msg_id < request_number =>
                    {
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
            Body::Prepare { op, .. } => {
                //TODO check if we need to catch up on previous ops
                self.op_number.fetch_add(1, Ordering::SeqCst);
                self.op_log.lock().unwrap().push(*op.clone());
                let response_body = self.clone().apply_to_state_machine(&op).await;
                self.client_table.lock().unwrap().insert(
                    op.src,
                    ClientTableEntry {
                        request_number: msg.body.msg_id,
                        response_body: Some(response_body.clone()),
                    },
                );
                self.node
                    .clone()
                    .send(
                        msg.src,
                        Body::PrepareOk {
                            in_reply_to: msg.body.msg_id,
                            view_numer: self.view_number.load(Ordering::SeqCst),
                            op_number: self.op_number.load(Ordering::SeqCst),
                        },
                        None,
                    )
                    .await;
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
                request_number: msg.body.msg_id,
                response_body: None,
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

        let response_body = self.clone().apply_to_state_machine(&msg).await;

        self.client_table.lock().unwrap().insert(
            msg.src.clone(),
            ClientTableEntry {
                request_number: msg.body.msg_id,
                response_body: Some(response_body.clone()),
            },
        );

        self.node
            .clone()
            .send(msg.src.clone(), response_body, None)
            .await;
    }

    async fn apply_to_state_machine(self: Arc<Self>, msg: &Message) -> Body {
        match msg.body.inner {
            Body::Read { key } => {
                let result = self.state_machine.lock().unwrap().read(&key).cloned();

                match result {
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
                }
            }
            Body::Write { key, value } => {
                self.clone().state_machine.lock().unwrap().write(key, value);
                Body::WriteOk {
                    in_reply_to: msg.body.msg_id,
                }
            }
            Body::Cas { key, from, to } => {
                let result = self
                    .clone()
                    .state_machine
                    .lock()
                    .unwrap()
                    .cas(key, from, to);

                match result {
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
                }
            }
            _ => unreachable!(),
        }
    }

    fn majority_count(self: Arc<Self>) -> usize {
        let all_nodes_count = self.node.other_node_ids.get().unwrap().len() + 1;
        (all_nodes_count / 2) + 1
    }
}
