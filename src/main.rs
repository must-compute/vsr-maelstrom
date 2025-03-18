mod kv_store;
mod message;
mod node;
mod vsr;

use std::sync::Arc;
use vsr::VSR;

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_level(false)
        .with_writer(std::io::stderr)
        .with_thread_ids(true)
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let node = Arc::new(VSR::new());
    node.run().await;
}
