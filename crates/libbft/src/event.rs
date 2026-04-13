use std::{any::type_name, collections::HashMap};

pub trait Event {
    type Type;
}

pub type EventSender<E> = tokio::sync::mpsc::Sender<(<E as Event>::Type, tracing::Span)>;
pub type EventReceiver<E> = tokio::sync::mpsc::Receiver<(<E as Event>::Type, tracing::Span)>;

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

pub trait EmitMap<K, E: Event> {
    fn tx_map_slot(&mut self) -> &mut Option<HashMap<K, EventSender<E>>>;

    fn set_tx_map(&mut self, tx_map: HashMap<K, EventSender<E>>) {
        let replaced = self.tx_map_slot().replace(tx_map);
        assert!(
            replaced.is_none(),
            "{} Tx map was already set",
            type_name::<E>()
        );
    }
}

impl<K, E: Event> EmitMap<K, E> for Option<HashMap<K, EventSender<E>>> {
    fn tx_map_slot(&mut self) -> &mut Option<HashMap<K, EventSender<E>>> {
        self
    }
}
