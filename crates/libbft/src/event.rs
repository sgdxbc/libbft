use std::{any::type_name, collections::HashMap};

use tokio::sync::mpsc::Sender;
use tracing::Span;

pub trait Event {
    type Type;
}

pub trait Emit<E: Event> {
    fn tx_slot(&mut self) -> &mut Option<Sender<(E::Type, Span)>>;

    fn set_tx(&mut self, tx: Sender<(E::Type, Span)>) {
        let replaced = self.tx_slot().replace(tx);
        assert!(
            replaced.is_none(),
            "{} Tx was already set",
            type_name::<E>()
        );
    }
}

impl<E: Event> Emit<E> for Option<Sender<(E::Type, Span)>> {
    fn tx_slot(&mut self) -> &mut Option<Sender<(E::Type, Span)>> {
        self
    }
}

pub trait EmitMap<K, E: Event> {
    fn tx_map_slot(&mut self) -> &mut Option<HashMap<K, Sender<(E::Type, Span)>>>;

    fn set_tx_map(&mut self, tx_map: HashMap<K, Sender<(E::Type, Span)>>) {
        let replaced = self.tx_map_slot().replace(tx_map);
        assert!(
            replaced.is_none(),
            "{} Tx map was already set",
            type_name::<E>()
        );
    }
}

impl<K, E: Event> EmitMap<K, E> for Option<HashMap<K, Sender<(E::Type, Span)>>> {
    fn tx_map_slot(&mut self) -> &mut Option<HashMap<K, Sender<(E::Type, Span)>>> {
        self
    }
}
