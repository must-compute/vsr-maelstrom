use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: BodyWithMsgId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct BodyWithMsgId {
    pub msg_id: usize,
    #[serde(flatten)]
    pub inner: Body,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Body {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        in_reply_to: usize,
    },
    Read {
        key: usize, // technically it should be Any
    },
    ReadOk {
        in_reply_to: usize,
        value: usize,
    },
    Write {
        key: usize, // technically it should be Any
        value: usize,
    },
    WriteOk {
        in_reply_to: usize,
    },
    Cas {
        key: usize, // technically it should be Any
        from: usize,
        to: usize,
    },
    CasOk {
        in_reply_to: usize,
    },
    Prepare {
        view_number: usize,
        op: Box<Message>,
        op_number: usize,
        commit_number: usize,
    },
    PrepareOk {
        view_number: usize,
        op_number: usize,
    },
    Proxy {
        proxied_msg: Box<Message>,
    },
    Commit {
        view_number: usize,
        commit_number: usize,
    },
    GetState {
        view_number: usize,
        op_number: usize,
    },
    NewState {
        in_reply_to: usize,
        view_number: usize,
        missing_log_suffix: Vec<Message>,
        op_number: usize,
        commit_number: usize,
    },
    StartViewChange {
        view_number: usize,
    },
    DoViewChange {
        view_number: usize,
        log: Vec<Message>,
        last_normal_status_view_number: usize,
        op_number: usize,
        commit_number: usize,
    },
    StartView {
        view_number: usize,
        log: Vec<Message>,
        op_number: usize,
        commit_number: usize,
    },
    Error {
        in_reply_to: usize,
        code: ErrorCode,
        text: String,
    },
}

impl Body {
    pub fn in_reply_to(&self) -> Option<usize> {
        match self {
            Body::ReadOk { in_reply_to, .. }
            | Body::WriteOk { in_reply_to, .. }
            | Body::CasOk { in_reply_to, .. }
            | Body::NewState { in_reply_to, .. }
            | Body::Error { in_reply_to, .. } => Some(*in_reply_to),
            Body::Init { .. }
            | Body::InitOk { .. }
            | Body::Read { .. }
            | Body::Write { .. }
            | Body::Cas { .. }
            | Body::Proxy { .. }
            | Body::Commit { .. }
            | Body::GetState { .. }
            | Body::StartViewChange { .. }
            | Body::DoViewChange { .. }
            | Body::StartView { .. }
            | Body::Prepare { .. }
            | Body::PrepareOk { .. } => None,
        }
    }
    pub fn set_in_reply_to(&mut self, new_in_reply_to: usize) {
        match self {
            Body::InitOk {
                ref mut in_reply_to,
                ..
            }
            | Body::ReadOk {
                ref mut in_reply_to,
                ..
            }
            | Body::WriteOk {
                ref mut in_reply_to,
                ..
            }
            | Body::CasOk {
                ref mut in_reply_to,
                ..
            }
            | Body::NewState {
                ref mut in_reply_to,
                ..
            }
            | Body::Error {
                ref mut in_reply_to,
                ..
            } => {
                *in_reply_to = new_in_reply_to;
            }
            Body::Init { .. }
            | Body::Read { .. }
            | Body::Write { .. }
            | Body::Cas { .. }
            | Body::Prepare { .. }
            | Body::PrepareOk { .. }
            | Body::Commit { .. }
            | Body::GetState { .. }
            | Body::StartViewChange { .. }
            | Body::DoViewChange { .. }
            | Body::StartView { .. }
            | Body::Proxy { .. } => {
                panic!("trying to set in_reply_to on a body that doesnt have such field")
            }
        }
    }
}

// https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#errors
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Clone)]
#[repr(u8)]
pub enum ErrorCode {
    Timeout = 0,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    PreconditionFailed = 22,
    TxnConflict = 30,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::Timeout => write!(f, "timeout"),
            ErrorCode::NotSupported => write!(f, "not supported"),
            ErrorCode::TemporarilyUnavailable => write!(f, "temporarily unavailable"),
            ErrorCode::MalformedRequest => write!(f, "malformed request"),
            ErrorCode::Crash => write!(f, "crash"),
            ErrorCode::Abort => write!(f, "abort"),
            ErrorCode::KeyDoesNotExist => write!(f, "key does not exist"),
            ErrorCode::PreconditionFailed => write!(f, "precondition failed"),
            ErrorCode::TxnConflict => write!(f, "txn conflict"),
        }
    }
}

impl std::error::Error for ErrorCode {}
