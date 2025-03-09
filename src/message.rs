use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Body {
    Init {
        msg_id: usize,
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    Read {
        msg_id: usize,
        key: usize, // technically it should be Any
    },
    ReadOk {
        msg_id: usize,
        in_reply_to: usize,
        value: serde_json::Value,
    },
    Write {
        msg_id: usize,
        key: usize, // technically it should be Any
        value: serde_json::Value,
    },
    WriteOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    Cas {
        msg_id: usize,
        key: usize, // technically it should be Any
        from: serde_json::Value,
        to: serde_json::Value,
    },
    CasOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    Prepare {
        msg_id: usize,
        view_number: usize,
        op: Box<Message>,
        op_number: usize,
        commit_number: usize,
    },
    PrepareOk {
        msg_id: usize,
        in_reply_to: usize,
        view_numer: usize,
        op_number: usize,
    },
    Error {
        in_reply_to: usize,
        code: ErrorCode,
        text: String,
    },
}

impl Body {
    pub fn msg_id(&self) -> usize {
        match self {
            Body::Init { msg_id, .. }
            | Body::InitOk { msg_id, .. }
            | Body::Read { msg_id, .. }
            | Body::ReadOk { msg_id, .. }
            | Body::Write { msg_id, .. }
            | Body::WriteOk { msg_id, .. }
            | Body::Cas { msg_id, .. }
            | Body::CasOk { msg_id, .. }
            | Body::Prepare { msg_id, .. }
            | Body::PrepareOk { msg_id, .. } => *msg_id,
            Body::Error { .. } => panic!("error msgs have no msg id"),
        }
    }
    pub fn set_msg_id(&mut self, new_msg_id: usize) {
        match self {
            Body::Init { ref mut msg_id, .. }
            | Body::InitOk { ref mut msg_id, .. }
            | Body::Read { ref mut msg_id, .. }
            | Body::ReadOk { ref mut msg_id, .. }
            | Body::Write { ref mut msg_id, .. }
            | Body::WriteOk { ref mut msg_id, .. }
            | Body::Cas { ref mut msg_id, .. }
            | Body::CasOk { ref mut msg_id, .. }
            | Body::Prepare { ref mut msg_id, .. }
            | Body::PrepareOk { ref mut msg_id, .. } => *msg_id = new_msg_id,
            Body::Error { .. } => panic!("error msgs have no msg id"),
        }
    }
    pub fn in_reply_to(&self) -> usize {
        match self {
            Body::ReadOk { in_reply_to, .. }
            | Body::WriteOk { in_reply_to, .. }
            | Body::CasOk { in_reply_to, .. }
            | Body::PrepareOk { in_reply_to, .. }
            | Body::Error { in_reply_to, .. } => *in_reply_to,
            Body::Init { .. }
            | Body::InitOk { .. }
            | Body::Read { .. }
            | Body::Write { .. }
            | Body::Cas { .. }
            | Body::Prepare { .. } => panic!("in_reply_to not supported for {:?}", self),
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
            | Body::PrepareOk {
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
            | Body::Prepare { .. } => {
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
