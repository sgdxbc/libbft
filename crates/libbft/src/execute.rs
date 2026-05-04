pub mod events {
    use tokio::sync::oneshot;

    use crate::{common::Payload, event::Event};

    pub struct Execute;
    impl Event for Execute {
        type Value = (Vec<Payload>, oneshot::Sender<Option<Vec<Payload>>>);
    }
}
