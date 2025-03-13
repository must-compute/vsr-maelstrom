use core::panic;
use std::{
    cmp::max,
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::{
    sync::Notify,
    time::{timeout, Instant},
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

#[derive(Clone, Debug, PartialEq)]
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
    status: Mutex<NodeStatus>,
    configuration: Mutex<Vec<NodeName>>, // All nodes in the cluster
    replica_number: AtomicUsize,
    client_table: Mutex<HashMap<ClientName, ClientTableEntry>>,
    node: Arc<Node>,
    state_machine: Mutex<KeyValueStore<StateMachineKey, StateMachineValue>>,
    reset_commit_msg_deadline_notifier: Notify,
    reset_view_change_deadline_notifier: Notify,
    last_normal_status_view_number: AtomicUsize,
    start_view_change_accumulator: AtomicUsize,
    do_view_change_accumulator: Mutex<Vec<Message>>,
}

impl VSR {
    pub fn new() -> Self {
        VSR {
            op_log: Mutex::new(Vec::new()),
            op_number: AtomicUsize::new(0),
            commit_number: AtomicUsize::new(0),
            view_number: AtomicUsize::new(0),
            status: Mutex::new(NodeStatus::Normal),
            configuration: Default::default(),
            replica_number: AtomicUsize::new(0),
            client_table: Default::default(),
            node: Arc::new(Node::new()),
            state_machine: Default::default(),
            reset_commit_msg_deadline_notifier: Notify::new(),
            reset_view_change_deadline_notifier: Notify::new(),
            last_normal_status_view_number: AtomicUsize::default(),
            start_view_change_accumulator: AtomicUsize::default(),
            do_view_change_accumulator: Mutex::new(Vec::new()),
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut rx = self.node.clone().run().await;

        let commit_msg_deadline_duration = Duration::from_millis(100);
        let view_change_deadline_duration = commit_msg_deadline_duration * 2;

        let mut commit_msg_deadline = tokio::time::interval_at(
            Instant::now() + commit_msg_deadline_duration,
            commit_msg_deadline_duration,
        );

        let mut view_change_deadline = tokio::time::interval_at(
            Instant::now() + view_change_deadline_duration,
            view_change_deadline_duration,
        );

        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    tokio::spawn({
                        let vsr = self.clone();
                        async move { vsr.handle(msg).await }
                    });
                }
                _ = commit_msg_deadline.tick() => {
                    // There is a chance this might tick before init msg is received.
                    // We infer that init is done via my_id being Some.
                    if let Some(my_id) = self.node.my_id.get() {
                        let leader_id = self.current_primary_node();
                        if *my_id == leader_id{
                            self.clone().broadcast_commit_msg().await;
                        }
                    }
                }
                _ = self.reset_commit_msg_deadline_notifier.notified() => {
                    commit_msg_deadline.reset();
                }
                _ = view_change_deadline.tick() => {
                    self.clone().perform_view_change_if_needed().await;

                }
                _ = self.reset_view_change_deadline_notifier.notified() => {
                    view_change_deadline.reset();
                }

            };
        }
    }

    fn current_primary_node(&self) -> String {
        self.primary_node_at_view_number(self.view_number.load(Ordering::SeqCst))
    }

    fn primary_node_at_view_number(&self, view_number: usize) -> String {
        let configuration = self.configuration.lock().unwrap();
        configuration
            .get(view_number % configuration.len())
            .cloned()
            .unwrap()
    }

    async fn perform_view_change_if_needed(self: Arc<Self>) {
        let my_id = self.node.my_id.get();

        //we are not initialized yet
        if my_id.is_none() {
            return;
        }

        //we are the leader, no need to change view
        if *my_id.unwrap() == self.current_primary_node() {
            return;
        }

        //happy path
        tracing::debug!("view change deadline elapsed. Initiating view change protocol");

        //TODO Record last normal
        self.view_number.fetch_add(1, Ordering::SeqCst);
        self.switch_node_status_to(NodeStatus::ViewChange);

        self.node
            .clone()
            .broadcast(
                Body::StartViewChange {
                    view_number: self.view_number.load(Ordering::SeqCst),
                },
                None,
            )
            .await;
    }

    fn switch_node_status_to(&self, new_status: NodeStatus) {
        match &new_status {
            NodeStatus::Normal => self
                .last_normal_status_view_number
                .store(self.view_number.load(Ordering::SeqCst), Ordering::SeqCst),
            _ => (),
        };
        *self.status.lock().unwrap() = new_status;
    }

    async fn broadcast_commit_msg(self: Arc<Self>) {
        let commit_number = self.commit_number.load(Ordering::SeqCst);

        let body = Body::Commit {
            view_number: self.view_number.load(Ordering::SeqCst),
            commit_number,
        };
        self.node.clone().broadcast(body, None).await;
    }

    async fn handle(self: Arc<Self>, msg: Message) -> anyhow::Result<()> {
        match msg.body.inner.clone() {
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
                let leader_id = self.current_primary_node();
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
                let leader_id = self.current_primary_node();
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

                let response = {
                    let client_table_guard = self.client_table.lock().unwrap();
                    client_table_guard.get(&msg.src).cloned()
                };

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

                    Some(ClientTableEntry {
                        request_number,
                        response_body: None,
                    }) if msg.body.msg_id >= request_number => {
                        let error_text =
                            String::from("Normal Protocol: received more than one in-flight request by the same client");
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
            Body::Prepare {
                op,
                op_number,
                view_number,
                ..
            } => {
                if view_number >= self.view_number.load(Ordering::SeqCst) {
                    self.reset_view_change_deadline_notifier.notify_one();
                }

                if op_number > self.op_number.load(Ordering::SeqCst) + 1 {
                    self.clone().catch_up().await;
                }

                self.clone().prepare_op(&op).await;

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
            Body::Commit {
                view_number,
                commit_number,
            } => {
                if view_number >= self.view_number.load(Ordering::SeqCst) {
                    self.reset_view_change_deadline_notifier.notify_one();
                }

                let my_op_number = self.op_number.load(Ordering::SeqCst);
                if commit_number > self.commit_number.load(Ordering::SeqCst) {
                    if commit_number > my_op_number {
                        self.clone().catch_up().await;
                    } else {
                        let client_request = self.op_log.lock().unwrap().last().cloned().unwrap();
                        self.commit_op(&client_request).await;
                    }
                }
            }
            Body::GetState {
                view_number,
                op_number,
            } => {
                let should_ignore = *self.status.lock().unwrap() != NodeStatus::Normal
                    || self.view_number.load(Ordering::SeqCst) != view_number;
                if should_ignore {
                    tracing::debug!("Ignoring GetState request");
                    return Ok(());
                }

                let missing_log_suffix = self.op_log.lock().unwrap()[op_number..].to_vec();

                let body = Body::NewState {
                    in_reply_to: msg.body.msg_id,
                    view_number: self.view_number.load(Ordering::SeqCst),
                    missing_log_suffix,
                    op_number: self.op_number.load(Ordering::SeqCst),
                    commit_number: self.commit_number.load(Ordering::SeqCst),
                };

                self.node.clone().send(msg.src, body, None).await;
            }
            Body::StartViewChange { view_number } => {
                let my_view_number = self.view_number.load(Ordering::SeqCst);
                let should_ignore = view_number < my_view_number;
                if should_ignore {
                    return Ok(());
                }

                if view_number > my_view_number {
                    // TODO switch to view change mode
                    self.view_number.store(view_number, Ordering::SeqCst);

                    {
                        let status_guard = self.status.lock().unwrap();
                        match *status_guard {
                            NodeStatus::Normal => {
                                self.switch_node_status_to(NodeStatus::ViewChange)
                            }
                            NodeStatus::ViewChange => (),
                            NodeStatus::Recovering => return Ok(()), // TODO are we sure?                    }
                        }
                    }

                    self.node
                        .clone()
                        .broadcast(
                            Body::StartViewChange {
                                view_number: self.view_number.load(Ordering::SeqCst),
                            },
                            None,
                        )
                        .await;

                    return Ok(());
                }

                // At this stage, we conclude that view_number == my_view_number.
                assert_eq!(*self.status.lock().unwrap(), NodeStatus::ViewChange);

                // TODO check if we got f StartViewChange msgs

                self.start_view_change_accumulator
                    .fetch_add(1, Ordering::SeqCst);

                //We count ourselves as already accumulated
                if self.start_view_change_accumulator.load(Ordering::SeqCst)
                    >= self.majority_count() - 1
                {
                    // self.reset_view_change_deadline_notifier.notify_one();

                    let primary = self.primary_node_at_view_number(view_number);

                    let body = Body::DoViewChange {
                        view_number,
                        log: self.op_log.lock().unwrap().clone(),
                        last_normal_status_view_number: self
                            .last_normal_status_view_number
                            .load(Ordering::SeqCst),
                        op_number: self.op_number.load(Ordering::SeqCst),
                        commit_number: self.commit_number.load(Ordering::SeqCst),
                    };

                    self.node.clone().send(primary, body, None).await;
                }
            }
            Body::DoViewChange {
                view_number,
                log,
                last_normal_status_view_number,
                op_number,
                commit_number,
            } => {
                let mut do_view_change_accumulator =
                    self.do_view_change_accumulator.lock().unwrap();

                do_view_change_accumulator.push(msg);

                if do_view_change_accumulator.len() < self.majority_count() {
                    return Ok(());
                }

                let status = self.status.lock().unwrap().clone();
                match status {
                    NodeStatus::Recovering => return Ok(()),
                    NodeStatus::Normal | NodeStatus::ViewChange => {
                        self.view_number.store(view_number, Ordering::SeqCst);

                        //TODO selects as the new log the one contained in
                        // the message with the largest v 0 ; if several messages
                        // have the same v 0 it selects the one among them with the largest n.  }

                        // self.op_number =
                    }
                }
            }
            _ => {
                todo!()
            }
        };
        Ok(())
    }

    async fn prepare_op(self: Arc<Self>, op: &Message) {
        self.op_number.fetch_add(1, Ordering::SeqCst);
        self.op_log.lock().unwrap().push(op.clone());

        self.client_table.lock().unwrap().insert(
            op.src.clone(),
            ClientTableEntry {
                request_number: op.body.msg_id,
                response_body: None,
            },
        );
    }

    async fn commit_op(self: Arc<Self>, op: &Message) {
        let response_body = self.clone().apply_to_state_machine(op).await;
        self.commit_number.fetch_add(1, Ordering::SeqCst);
        self.client_table.lock().unwrap().insert(
            op.src.clone(),
            ClientTableEntry {
                request_number: op.body.msg_id,
                response_body: Some(response_body),
            },
        );
    }

    async fn catch_up(self: Arc<Self>) {
        loop {
            let random_peer = self.node.get_random_peer();
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.node
                .clone()
                .send(
                    random_peer,
                    Body::GetState {
                        view_number: self.view_number.load(Ordering::SeqCst),
                        op_number: self.op_number.load(Ordering::SeqCst),
                    },
                    Some(tx),
                )
                .await;

            let timeout = timeout(Duration::from_millis(500), rx).await;
            match timeout {
                Ok(response) => {
                    let Body::NewState {
                        view_number,
                        missing_log_suffix,
                        op_number,
                        commit_number,
                        ..
                    } = response.unwrap().body.inner
                    else {
                        panic!("should receive a NewState as a response to GetState");
                    };

                    //TODO fix? (there might be an off-by one error in some case)
                    for op in &missing_log_suffix {
                        self.clone().prepare_op(op).await;
                        self.clone().commit_op(op).await;
                    }

                    assert_eq!(self.op_number.load(Ordering::SeqCst), op_number);
                    assert_eq!(self.commit_number.load(Ordering::SeqCst), commit_number);
                    break;
                }
                Err(_) => continue,
            }
        }
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

        self.reset_commit_msg_deadline_notifier.notify_one();

        // We count ourselves as part of the consensus majority
        let mut remaining_response_count = self.majority_count() - 1;

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

    fn majority_count(&self) -> usize {
        let all_nodes_count = self.node.other_node_ids.get().unwrap().len() + 1;
        (all_nodes_count / 2) + 1
    }
}
