use std::any::type_name;

use derive_where::derive_where;
use metrics::Gauge;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tracing::warn;

pub trait Event {
    type Value;
}

// currently go with tokio channel to avoid introducing more dependencies
// switch to async-channel when there's a solid use case of MPMC

pub struct EventChannel<E: Event> {
    pub tx: Sender<(E::Value, tracing::Span)>,
    pub rx: Receiver<(E::Value, tracing::Span)>,
    pub gauge: Option<Gauge>,
}

#[derive_where(Clone)]
pub struct EventSender<E: Event> {
    tx: Sender<(E::Value, tracing::Span)>,
    gauge: Option<Gauge>,
}

impl<E: Event> EventChannel<E> {
    pub fn new(gauge: Option<Gauge>) -> Self {
        let (tx, rx) = channel(1000);
        Self { tx, rx, gauge }
    }

    pub fn sender(&self) -> EventSender<E> {
        EventSender {
            tx: self.tx.clone(),
            gauge: self.gauge.clone(),
        }
    }

    pub async fn recv(&mut self) -> Option<(E::Value, tracing::Span)> {
        let event = self.rx.recv().await?;
        if let Some(gauge) = &self.gauge {
            gauge.decrement(1);
        }
        Some(event)
    }
}

impl<E: Event> EventSender<E> {
    pub async fn send(&self, value: E::Value, span: tracing::Span) -> bool {
        if let Some(gauge) = &self.gauge {
            gauge.increment(1);
        }
        if let Err(err) = self.tx.send((value, span)).await {
            warn!("Failed to send event of type {}: {err:#}", type_name::<E>());
            return false;
        }
        true
    }
}

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
