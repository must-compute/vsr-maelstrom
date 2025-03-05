use tokio::sync::mpsc::Sender;

use crate::message::Message;

// TODO use Result<T, Error>
type ChannelResponder<T> = tokio::sync::oneshot::Sender<T>;

pub enum Event {
    Call(Query),
    Cast(Command),
}

// For events with a notifier channel for response
pub enum Query {
    SendViaMaelstrom {
        message: Message,
        responder: ChannelResponder<Message>,
    },
}

// for events with no expected response
pub enum Command {
    ReceivedViaMaelstrom { response: Message },
    SendViaMaelstrom { message: Message },
}

pub async fn query<R>(
    event_tx: Sender<Event>,
    build_query: impl FnOnce(ChannelResponder<R>) -> Query,
) -> R {
    let (tx, rx) = tokio::sync::oneshot::channel();
    event_tx
        .send(Event::Call(build_query(tx)))
        .await
        .expect("should be able to send query event");
    rx.await
        .expect("should be able to receive query event response")
}
