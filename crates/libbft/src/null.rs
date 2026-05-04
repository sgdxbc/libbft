use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::event::{Emit, EventChannel};

pub mod events {
    pub type Execute = crate::execute::events::Execute;
}

pub struct Null {
    op: EventChannel<events::Execute>,
}

impl Null {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            op: EventChannel::new(None),
        }
    }

    pub fn register(&self, mut emit_op: impl Emit<events::Execute>) {
        emit_op.install(self.op.sender());
    }

    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some(((_ops, tx), _span))) = token.run_until_cancelled(self.op.recv()).await
        {
            if tx.send(None).is_err() {
                warn!("failed to send response to client");
            }
        }
    }
}
