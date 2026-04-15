pub mod events {
    use std::net::SocketAddr;

    use bytes::Bytes;

    use crate::event::Event;

    pub struct HandleBytes;
    impl Event for HandleBytes {
        type Type = Vec<u8>;
    }

    pub struct SendBytes;
    impl Event for SendBytes {
        type Type = (SocketAddr, Bytes);
    }
}
