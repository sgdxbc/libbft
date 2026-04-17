use std::any::type_name;

pub trait Event {
    type Value;
}

pub type EventSender<E> = tokio::sync::mpsc::Sender<(<E as Event>::Value, tracing::Span)>;
pub type EventReceiver<E> = tokio::sync::mpsc::Receiver<(<E as Event>::Value, tracing::Span)>;

pub trait Emit<E: Event> {
    fn install(&mut self, tx: EventSender<E>);

    fn and(self, other: impl Emit<E>) -> impl Emit<E>
    where
        Self: Sized,
    {
        EmitPair(self, other)
    }
}

pub trait EventSenderSlot<E: Event> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<E>>;
}

impl<E: Event> EventSenderSlot<E> for Option<EventSender<E>> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<E>> {
        self
    }
}

pub struct AsEmit<T>(pub T);

impl<E: Event, T: EventSenderSlot<E>> Emit<E> for AsEmit<&'_ mut T> {
    fn install(&mut self, tx: EventSender<E>) {
        let replaced = self.0.sender_slot().replace(tx);
        assert!(
            replaced.is_none(),
            "{} event sender already installed",
            type_name::<E>()
        );
    }
}

struct EmitPair<T1, T2>(T1, T2);

impl<E: Event, T: Emit<E>, U: Emit<E>> Emit<E> for EmitPair<T, U> {
    fn install(&mut self, tx: EventSender<E>) {
        self.0.install(tx.clone());
        self.1.install(tx);
    }
}
