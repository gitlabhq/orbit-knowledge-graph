//! Client side of the Workhorse Gitaly proxy: gRPC over a single HTTP/2
//! connection tunneled through one WebSocket per (process, project) session.
//!
//! The wire contract this implements is published as
//! `doc/development/workhorse/gitaly_proxy.md` in gitlab-org/gitlab. In short:
//! one upgrade = one preauth = one repository; session parameters arrive only
//! in the 101 headers; the client rotates at 90 % of the session age and
//! retries a stale rejection exactly once; only WS close 1000/1001 is EOF.

mod channel;
mod dial;
mod error;
mod websocket_io;

pub use channel::{GitalyProxyChannel, GitalyProxyChannels};
pub use dial::{
    Dialer as GitalyProxyDialer, HEADER_EXPIRES_IN, HEADER_PROFILE, HEADER_REPOSITORY,
    PROFILE_READONLY_REPOSITORY, Session as GitalyProxySession,
};
pub use error::{
    GitalyProxyError, PROXY_STATUS_PREFIX, REASON_SESSION_EXPIRED, REASON_SHUTTING_DOWN,
    REASON_STREAM_DEADLINE, StatusClass, classify_status, proxy_reason,
};
pub use websocket_io::WebSocketIo;
