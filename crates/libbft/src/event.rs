use std::any::type_name;

pub trait Event {
    type Value;
}

pub type EventSender<E> = tokio::sync::mpsc::Sender<(<E as Event>::Value, tracing::Span)>;
pub type EventReceiver<E> = tokio::sync::mpsc::Receiver<(<E as Event>::Value, tracing::Span)>;

pub trait Emit<E: Event> {
    fn tx_slot(&mut self) -> &mut Option<EventSender<E>>;

    fn set_tx(&mut self, tx: EventSender<E>) {
        let replaced = self.tx_slot().replace(tx);
        assert!(
            replaced.is_none(),
            "{} Tx was already set",
            type_name::<E>()
        );
    }
}

impl<E: Event> Emit<E> for Option<EventSender<E>> {
    fn tx_slot(&mut self) -> &mut Option<EventSender<E>> {
        self
    }
}
