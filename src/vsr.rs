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
    message::{Body, BodyWithMsgId, ErrorCode, Message},
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

struct MsgAccumulator {
    accumulated_msgs: Vec<Message>,
    is_done_processing: bool,
}

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
    prepare_ok_tracker: Mutex<HashMap<usize, MsgAccumulator>>, // k: op_number
    start_view_change_tracker: Mutex<HashMap<usize, MsgAccumulator>>, // k: view_number
    do_view_change_tracker: Mutex<HashMap<usize, MsgAccumulator>>, // k: view_number
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
            prepare_ok_tracker: Default::default(),
            start_view_change_tracker: Default::default(),
            do_view_change_tracker: Default::default(),
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut rx = self.node.clone().run().await;

        let commit_msg_deadline_duration = Duration::from_millis(100);
        let view_change_deadline_duration = commit_msg_deadline_duration * 10;

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
                        if *my_id == leader_id && *self.status.lock().unwrap() == NodeStatus::Normal {
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

        // We are the leader, no need to change view
        if *my_id.unwrap() == self.current_primary_node()
            && *self.status.lock().unwrap() == NodeStatus::Normal
        {
            return;
        }

        //happy path
        tracing::debug!("view change deadline elapsed. Initiating view change protocol");

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
                if *self.status.lock().unwrap() != NodeStatus::Normal {
                    tracing::debug!("ignoring write/read/cas because I am not a Primary that is within Normal Protocol");
                    return Ok(());
                }
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
                if *self.status.lock().unwrap() != NodeStatus::Normal {
                    tracing::debug!("ignoring Prepare msg because I am not in Normal status.");
                    return Ok(());
                }

                let my_view_number = self.view_number.load(Ordering::SeqCst);
                if view_number >= my_view_number {
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
                            view_number: self.view_number.load(Ordering::SeqCst),
                            op_number: self.op_number.load(Ordering::SeqCst),
                        },
                        None,
                    )
                    .await;
            }
            Body::PrepareOk {
                view_number,
                op_number,
            } => {
                assert_eq!(view_number, self.view_number.load(Ordering::SeqCst));
                let f = self.majority_count() - 1;
                let mut op_is_ready_to_commit = false;
                {
                    let mut tracker_guard = self.prepare_ok_tracker.lock().unwrap();
                    tracker_guard
                        .entry(op_number)
                        .and_modify(
                            |MsgAccumulator {
                                 ref mut accumulated_msgs,
                                 is_done_processing,
                             }| {
                                if *is_done_processing {
                                    // This PrepareOk msg can be safely discarded since it arrived after quorum
                                    // was achieved.
                                    // TODO this is a memory leak though. Maybe add periodic clean up.
                                    return;
                                }
                                accumulated_msgs.push(msg.clone());
                                if accumulated_msgs.len() >= f {
                                    op_is_ready_to_commit = true;
                                }
                            },
                        )
                        .or_insert_with(|| {
                            if 1 >= f {
                                op_is_ready_to_commit = true;
                            }
                            MsgAccumulator {
                                accumulated_msgs: vec![msg.clone()],
                                is_done_processing: false,
                            }
                        });
                }

                // If f nodes have prepared this op, we consider this op (and all earlier
                // non-committed ops) ready to commit.
                if op_is_ready_to_commit {
                    let my_commit_number = self.commit_number.load(Ordering::SeqCst);
                    let ops_to_commit = &self.op_log.lock().unwrap().clone()[my_commit_number..]
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>();

                    for op in ops_to_commit {
                        self.clone().commit_op(&op).await;

                        // mark op as committed in our tracker, so that we ignore any
                        // remaining PrepareOk msgs for this op.
                        self.prepare_ok_tracker
                            .lock()
                            .unwrap()
                            .entry(op_number)
                            .and_modify(
                                |MsgAccumulator {
                                     ref mut is_done_processing,
                                     ..
                                 }| {
                                    *is_done_processing = true;
                                },
                            );

                        let response = self
                            .client_table
                            .lock()
                            .unwrap()
                            .get(&op.src)
                            .unwrap() // entry exists, due to commit_op()
                            .clone()
                            .response_body
                            .unwrap(); // entry exists, due to commit_op()
                        self.node.clone().send(op.src.clone(), response, None).await;
                    }
                }
            }
            Body::Commit {
                view_number,
                commit_number,
            } => {
                if *self.status.lock().unwrap() != NodeStatus::Normal {
                    tracing::debug!("ignoring Commit msg because I am not in Normal status.");
                    return Ok(());
                }

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
                // GetState should only be processed in Normal mode at the same view number (for now)
                // and only by a replica.
                let should_ignore = *self.status.lock().unwrap() != NodeStatus::Normal
                    || self.view_number.load(Ordering::SeqCst) != view_number
                    || *self.node.my_id.get().unwrap() == self.current_primary_node();
                if should_ignore {
                    tracing::debug!("Ignoring GetState request");
                    return Ok(());
                }

                assert!(op_number < self.op_number.load(Ordering::SeqCst));
                let missing_log_suffix = self.op_log.lock().unwrap()[op_number..]
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();

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
                    tracing::debug!("ignoring StartViewChange message because the incoming view_number: {view_number} is lower than my view number {my_view_number}");
                    return Ok(());
                }

                if view_number > my_view_number {
                    tracing::debug!("sending StartViewChange message because the incoming view_number: {view_number} is higher than my view number {my_view_number}");
                    self.reset_view_change_deadline_notifier.notify_one();
                    self.view_number.store(view_number, Ordering::SeqCst);
                    let my_status = self.status.lock().unwrap().clone();
                    match my_status {
                        NodeStatus::Normal => self.switch_node_status_to(NodeStatus::ViewChange),
                        NodeStatus::ViewChange => (),
                        NodeStatus::Recovering => return Ok(()), // TODO are we sure?                    }
                    }
                    tracing::debug!("UNLOCKED");

                    self.node
                        .clone()
                        .broadcast(
                            Body::StartViewChange {
                                view_number: self.view_number.load(Ordering::SeqCst),
                            },
                            None,
                        )
                        .await;

                    tracing::debug!("DONE BROADCASTING SVC [in response to receiving a SVC with view_number higher than mine]");
                    return Ok(());
                }

                // At this stage, we conclude that view_number == my_view_number.
                // assert_eq!(*self.status.lock().unwrap(), NodeStatus::ViewChange);

                let f = self.majority_count() - 1;
                let mut can_send_do_view_change = false;
                {
                    let mut tracker_guard = self.start_view_change_tracker.lock().unwrap();
                    tracker_guard
                        .entry(view_number)
                        .and_modify(
                            |MsgAccumulator {
                                 ref mut accumulated_msgs,
                                 is_done_processing,
                             }| {
                                if *is_done_processing {
                                    // TODO Maybe add periodic clean up.
                                    return;
                                }
                                accumulated_msgs.push(msg.clone());
                                if accumulated_msgs.len() >= f {
                                    can_send_do_view_change = true;
                                }
                            },
                        )
                        .or_insert_with(|| {
                            if 1 >= f {
                                can_send_do_view_change = true;
                            }
                            MsgAccumulator {
                                accumulated_msgs: vec![msg.clone()],
                                is_done_processing: false,
                            }
                        });
                }

                if can_send_do_view_change {
                    self.prepare_ok_tracker
                        .lock()
                        .unwrap()
                        .entry(view_number)
                        .and_modify(
                            |MsgAccumulator {
                                 ref mut is_done_processing,
                                 ..
                             }| {
                                *is_done_processing = true;
                            },
                        );

                    self.reset_view_change_deadline_notifier.notify_one();

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
                view_number: latest_view_number,
                ..
            } => {
                let mut existing_commit_number = self.commit_number.load(Ordering::SeqCst);
                let mut updated_commit_number = 0;

                let status = self.status.lock().unwrap().clone();
                match status {
                    NodeStatus::Recovering => return Ok(()),
                    NodeStatus::Normal | NodeStatus::ViewChange => {
                        if latest_view_number > self.view_number.load(Ordering::SeqCst) {
                            tracing::debug!(
                                "initiating StartViewChange because: received DoViewChange with a view_number: {latest_view_number}, higher than my view number: {:?}",
                                self.view_number.load(Ordering::SeqCst));
                            self.view_number.store(latest_view_number, Ordering::SeqCst);
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
                            return Ok(());
                        }

                        // TODO after here, return early if I'm not the candidate.
                        // TODO any other checks to make sure the do view change is addressing me.
                        let mut can_start_view = false;
                        {
                            let mut tracker_guard = self.do_view_change_tracker.lock().unwrap();
                            tracker_guard
                                .entry(latest_view_number)
                                .and_modify(
                                    |MsgAccumulator {
                                         ref mut accumulated_msgs,
                                         is_done_processing,
                                     }| {
                                        if *is_done_processing {
                                            // TODO Maybe add periodic clean up.
                                            return;
                                        }
                                        accumulated_msgs.push(msg.clone());
                                        if accumulated_msgs.len() >= self.majority_count() {
                                            tracing::debug!("can start view now, because majority count is {:?}", self.majority_count());
                                            can_start_view = true;
                                        }
                                    },
                                )
                                .or_insert_with(||{
                                    if 1 >= self.majority_count() {
                                        tracing::debug!("can start view now, because majority count is {:?}", self.majority_count());
                                        can_start_view = true;
                                    }
                                    MsgAccumulator {
                                    accumulated_msgs: vec![msg.clone()],
                                    is_done_processing: false,
                                }});
                        }

                        if can_start_view {
                            self.prepare_ok_tracker
                                .lock()
                                .unwrap()
                                .entry(latest_view_number)
                                .and_modify(
                                    |MsgAccumulator {
                                         ref mut is_done_processing,
                                         ..
                                     }| {
                                        *is_done_processing = true;
                                    },
                                );

                            let msgs = self
                                .do_view_change_tracker
                                .lock()
                                .unwrap()
                                .get(&latest_view_number)
                                .unwrap()
                                .accumulated_msgs
                                .clone();

                            let all_do_view_change_msgs_have_identical_view_numbers =
                                msgs.iter().all(|msg| {
                                    let Body::DoViewChange { view_number, .. } = msg.body.inner
                                    else {
                                        unreachable!()
                                    };

                                    view_number == latest_view_number
                                });
                            assert!(all_do_view_change_msgs_have_identical_view_numbers);
                            self.view_number.store(latest_view_number, Ordering::SeqCst);

                            // We need to choose the latest log.
                            let mut logs_to_choose_from = msgs
                                .iter()
                                .map(|msg| {
                                    let Body::DoViewChange {
                                        last_normal_status_view_number,
                                        op_number,
                                        log,
                                        ..
                                    } = &msg.body.inner
                                    else {
                                        unreachable!()
                                    };
                                    // This tuple will help us find the latest log.
                                    (
                                        last_normal_status_view_number, // the paper calls this: v'
                                        op_number,                      // <-- and calls this: n
                                        log,
                                    )
                                })
                                .collect::<Vec<(_, _, _)>>();
                            logs_to_choose_from
                                .sort_by(|b, a| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
                            // we sorted by descending order, so we take the last log in the sorted logs.
                            let (_, _, latest_log) = logs_to_choose_from.first().unwrap();
                            *self.op_log.lock().unwrap() = latest_log.to_vec();

                            let latest_op_number = latest_log.len();
                            self.op_number.store(latest_op_number, Ordering::SeqCst);

                            let latest_commit_number = msgs
                                .iter()
                                .map(|msg| {
                                    let Body::DoViewChange { commit_number, .. } = msg.body.inner
                                    else {
                                        unreachable!()
                                    };
                                    commit_number
                                })
                                .max()
                                .unwrap();
                            existing_commit_number = self
                                .commit_number
                                .swap(latest_commit_number, Ordering::SeqCst);
                            updated_commit_number = latest_commit_number;
                        }

                        self.switch_node_status_to(NodeStatus::Normal);

                        let body = Body::StartView {
                            view_number: self.view_number.load(Ordering::SeqCst),
                            log: self.op_log.lock().unwrap().clone(),
                            op_number: self.op_number.load(Ordering::SeqCst),
                            commit_number: self.commit_number.load(Ordering::SeqCst),
                        };
                        self.node.clone().broadcast(body, None).await;

                        // The new primary executes any commited operations it hadn't executed previously.
                        // This is per step 4 of the VSR paper.
                        // TODO Here the paper states "updates its client table, and sends the
                        //      replies to the clients". This seems to assume that clients
                        //      might be ok with potentially receiving the same reply more than once
                        //      but I'm not sure yet. Skipping this tiny step for now.
                        let op_log = self.op_log.lock().unwrap().clone();
                        if updated_commit_number != 0 {
                            for op in &op_log[existing_commit_number..=updated_commit_number] {
                                self.clone().prepare_op_no_increment(op).await;
                                self.clone().commit_op_no_increment(op).await;
                            }
                        }

                        // At this point, there _might_ be some ops in the operation log that haven't been commited.
                        // Upon receiving StartView, replicas will send PrepareOk with the latest op number if they
                        // notice any uncomitted ops, which will cause the primary to commit all of those uncomitted ops
                        // in order (as usual for PrepareOk).
                    }
                }
            }
            Body::StartView {
                view_number,
                log,
                op_number,
                commit_number,
            } => {
                self.reset_view_change_deadline_notifier.notify_one();

                assert!(view_number >= self.view_number.load(Ordering::SeqCst));
                self.view_number.store(view_number, Ordering::SeqCst);

                let old_log = self.op_log.lock().unwrap().clone();
                let my_commit_number = self.commit_number.load(Ordering::SeqCst);

                if !old_log.is_empty() {
                    assert!(my_commit_number <= commit_number);
                    let my_committed_ops = &old_log[0..my_commit_number];
                    let remote_ops_until_my_commit_number = &log[0..my_commit_number];
                    assert_eq!(my_committed_ops, remote_ops_until_my_commit_number);
                }

                // 1. remove all uncommitted ops from old log
                *self.op_log.lock().unwrap() = old_log[0..my_commit_number].to_vec();

                let mut remaining_op_count_for_comitting = commit_number - my_commit_number;

                // 2. slice from old commit number till end of new log
                for op in &log[my_commit_number..] {
                    // prepare each of those ops, insert to client table
                    self.clone().prepare_op(op).await;
                    // commit only if within new commit number
                    if remaining_op_count_for_comitting > 0 {
                        self.clone().commit_op(op).await;
                        remaining_op_count_for_comitting -= 1;
                    }
                }

                assert_eq!(self.op_number.load(Ordering::SeqCst), op_number, "applied StartView but my op_number (left) failed to match the Primary op_number");
                assert_eq!(
                    self.commit_number.load(Ordering::SeqCst),
                    commit_number,
                    "applied StartView but my commit_number (left) failed to match the Primary commit_number"
                );

                if op_number > commit_number {
                    let body = Body::PrepareOk {
                        view_number,
                        op_number,
                    };
                    self.node.clone().send(msg.src, body, None).await;
                }

                self.switch_node_status_to(NodeStatus::Normal);
            }
            _ => {
                todo!()
            }
        };
        Ok(())
    }

    //TODO remove async
    async fn prepare_op(self: Arc<Self>, op: &Message) {
        self.clone().prepare_op_no_increment(op).await;
        let existing_op_number = self.op_number.fetch_add(1, Ordering::SeqCst);
        tracing::debug!(
            "incremented my op_number from {existing_op_number} while preparing {:?}",
            op
        );
    }

    async fn prepare_op_no_increment(self: Arc<Self>, op: &Message) {
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
        self.clone().commit_op_no_increment(op).await;
        let existing_commit_number = self.commit_number.fetch_add(1, Ordering::SeqCst);
        tracing::debug!(
            "incremented my commit_number from {existing_commit_number} while committing {:?}",
            op
        );
    }

    async fn commit_op_no_increment(self: Arc<Self>, op: &Message) {
        let response_body = self.clone().apply_to_state_machine(op).await;
        self.client_table.lock().unwrap().insert(
            op.src.clone(),
            ClientTableEntry {
                request_number: op.body.msg_id,
                response_body: Some(response_body),
            },
        );
    }

    async fn catch_up(self: Arc<Self>) {
        assert_eq!(*self.status.lock().unwrap(), NodeStatus::Normal);
        loop {
            let mut random_peer_replica = self.node.get_random_peer();
            while random_peer_replica == self.current_primary_node() {
                random_peer_replica = self.node.get_random_peer();
            }

            let (tx, rx) = tokio::sync::oneshot::channel();
            self.node
                .clone()
                .send(
                    random_peer_replica,
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

                    assert!(view_number >= self.view_number.load(Ordering::SeqCst));

                    let mut remaining_op_count_for_comitting =
                        commit_number - self.commit_number.load(Ordering::SeqCst);

                    for op in &missing_log_suffix {
                        self.clone().prepare_op(op).await;
                        if remaining_op_count_for_comitting > 0 {
                            self.clone().commit_op(op).await;
                            remaining_op_count_for_comitting -= 1;
                        }
                    }

                    assert_eq!(
                        self.op_number.load(Ordering::SeqCst),
                        op_number,
                        "catch up: operation numbers must match after catching up (left is my op_number)"
                    );
                    assert_eq!(
                        self.commit_number.load(Ordering::SeqCst),
                        commit_number,
                        "catch up: commit numbers must match after catching up (left is my commit_number)"
                    );
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
        let body = Body::Prepare {
            view_number: self.view_number.load(Ordering::SeqCst),
            op: Box::new(msg.clone()),
            op_number: self.op_number.load(Ordering::SeqCst),
            commit_number: self.commit_number.load(Ordering::SeqCst),
        };
        self.node.clone().broadcast(body, None).await;

        self.reset_commit_msg_deadline_notifier.notify_one();
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
