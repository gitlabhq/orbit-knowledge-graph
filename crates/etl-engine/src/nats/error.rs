//! Map async-nats errors to BrokerError.

use crate::message_broker::BrokerError;

pub(crate) fn map_connect_error(error: async_nats::ConnectError) -> BrokerError {
    BrokerError::Connection(error.to_string())
}

pub(crate) fn map_subscribe_error<E: std::fmt::Display>(error: E) -> BrokerError {
    BrokerError::Subscribe(error.to_string())
}

pub(crate) fn map_ack_error(error: async_nats::Error) -> BrokerError {
    BrokerError::Ack(error.to_string())
}

pub(crate) fn map_nack_error(error: async_nats::Error) -> BrokerError {
    BrokerError::Nack(error.to_string())
}
