use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use gitaly_protos::proto::Repository;
use hyper_util::rt::TokioIo;
use tonic::Status;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;
use tracing::{debug, warn};

use super::dial::{Dialer, Session};
use super::error::{GitalyProxyError, StatusClass, classify_status};

/// tonic needs a URI for the endpoint, but every byte goes through the
/// connector; nothing is ever resolved or dialed by name.
const ENDPOINT_URI: &str = "http://gitaly-proxy.invalid";

/// Client-side h2 PING cadence (wire contract §1: ~20 s, 10 s timeout, also
/// while idle). Keeps intermediaries from idling the socket out.
const H2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(20);
const H2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Rotation margin (wire contract §6.1): rotate at 90 % of the session age,
/// but never closer than 5 s to expiry.
const ROTATION_FRACTION: u32 = 10;
const ROTATION_MIN_MARGIN: Duration = Duration::from_secs(5);

/// One WebSocket-tunneled gRPC connection authorized for one repository.
///
/// Holders keep it alive for the life of their stream via the `Arc`; the
/// socket closes when the last holder drops it. The rotation slot stops
/// handing it out for new RPCs at [`GitalyProxyChannel::rotate_at`].
pub struct GitalyProxyChannel {
    project_id: i64,
    channel: Channel,
    session: Arc<RwLock<Session>>,
}

impl GitalyProxyChannel {
    async fn connect(dialer: Arc<Dialer>, project_id: i64) -> Result<Self, GitalyProxyError> {
        let (io, session) = dialer.handshake(project_id).await?;
        let session = Arc::new(RwLock::new(session));

        // The first handshake runs outside tonic so its typed error and the
        // session parameters are available before a channel exists. tonic's
        // own re-dial after a dropped connection (a backstop, not the rotation
        // mechanism) goes through the same handshake and refreshes the session.
        let first_io = Arc::new(Mutex::new(Some(io)));
        let connector_session = Arc::clone(&session);
        let connector = service_fn(move |_uri| {
            let dialer = Arc::clone(&dialer);
            let first_io = Arc::clone(&first_io);
            let session = Arc::clone(&connector_session);
            async move {
                let taken = first_io.lock().unwrap_or_else(|e| e.into_inner()).take();
                let io = match taken {
                    Some(io) => io,
                    None => {
                        debug!(project_id, "re-dialing gitaly proxy session");
                        let (io, fresh) = dialer.handshake(project_id).await?;
                        *session.write().unwrap_or_else(|e| e.into_inner()) = fresh;
                        io
                    }
                };
                Ok::<_, GitalyProxyError>(TokioIo::new(io))
            }
        });

        let channel = Endpoint::from_static(ENDPOINT_URI)
            .http2_keep_alive_interval(H2_KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(H2_KEEP_ALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .connect_with_connector(connector)
            .await?;

        Ok(Self {
            project_id,
            channel,
            session,
        })
    }

    pub fn project_id(&self) -> i64 {
        self.project_id
    }

    /// The tonic channel. Clone it into a generated client; per-RPC timeouts
    /// are the caller's.
    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }

    /// Coordinates every request on this channel must carry, verbatim from
    /// the 101 response. Never derive them from anywhere else.
    pub fn repository(&self) -> Repository {
        self.session().repository
    }

    pub fn expires_at(&self) -> Instant {
        self.session().expires_at()
    }

    /// When the slot stops handing this channel out for new streams.
    pub fn rotate_at(&self) -> Instant {
        rotate_at(&self.session())
    }

    pub fn accepts_new_streams(&self) -> bool {
        Instant::now() < self.rotate_at()
    }

    fn session(&self) -> Session {
        self.session
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

fn rotate_at(session: &Session) -> Instant {
    let margin = (session.expires_in / ROTATION_FRACTION).max(ROTATION_MIN_MARGIN);
    session.connected_at + session.expires_in.saturating_sub(margin)
}

/// The current channel for one project plus the single-flight lock for
/// dialing its replacement.
#[derive(Default)]
struct ProjectSlot {
    current: Mutex<Option<Arc<GitalyProxyChannel>>>,
    dialing: tokio::sync::Mutex<()>,
}

impl ProjectSlot {
    fn fresh(&self) -> Option<Arc<GitalyProxyChannel>> {
        self.current
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .filter(|channel| channel.accepts_new_streams())
            .cloned()
    }

    async fn get_or_dial(
        &self,
        dialer: &Arc<Dialer>,
        project_id: i64,
    ) -> Result<Arc<GitalyProxyChannel>, GitalyProxyError> {
        if let Some(channel) = self.fresh() {
            return Ok(channel);
        }
        let _dialing = self.dialing.lock().await;
        if let Some(channel) = self.fresh() {
            return Ok(channel);
        }
        let channel = Arc::new(GitalyProxyChannel::connect(Arc::clone(dialer), project_id).await?);
        *self.current.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&channel));
        Ok(channel)
    }

    /// Drops `stale` from the slot if it is still the current channel, so the
    /// next call dials. A holder that already replaced it is left alone.
    fn invalidate(&self, stale: &Arc<GitalyProxyChannel>) {
        let mut current = self.current.lock().unwrap_or_else(|e| e.into_inner());
        if current
            .as_ref()
            .is_some_and(|channel| Arc::ptr_eq(channel, stale))
        {
            *current = None;
        }
    }
}

/// Per-project rotating channels. Rotation only: no idle eviction, no cap,
/// no dial admission control — those belong to the channel cache that the
/// webserver path adds on top of this.
pub struct GitalyProxyChannels {
    dialer: Arc<Dialer>,
    slots: Mutex<HashMap<i64, Arc<ProjectSlot>>>,
}

impl GitalyProxyChannels {
    pub fn new(dialer: Dialer) -> Self {
        Self {
            dialer: Arc::new(dialer),
            slots: Mutex::new(HashMap::new()),
        }
    }

    fn slot(&self, project_id: i64) -> Arc<ProjectSlot> {
        Arc::clone(
            self.slots
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(project_id)
                .or_default(),
        )
    }

    /// Returns the current channel for `project_id` while it is fresh, or
    /// dials a replacement (a new upgrade, a new preauth) and swaps it in.
    pub async fn get(&self, project_id: i64) -> Result<Arc<GitalyProxyChannel>, GitalyProxyError> {
        self.slot(project_id)
            .get_or_dial(&self.dialer, project_id)
            .await
    }

    /// Runs `rpc` on the project's channel and, if the proxy rejects the
    /// stream as stale (`session_expired` / `shutting_down`), rotates and
    /// runs it once more on a fresh channel.
    ///
    /// Only the *initial* result of `rpc` is classified: a server-streaming
    /// call returns once the response headers arrive, and a rejection by the
    /// proxy is always trailers-only, so nothing that reached Gitaly is ever
    /// re-sent. Errors surfaced later by the response stream are the caller's
    /// mid-stream retry decision, not this helper's.
    pub async fn with_channel<T, F, Fut>(
        &self,
        project_id: i64,
        mut rpc: F,
    ) -> Result<T, GitalyProxyError>
    where
        F: FnMut(Arc<GitalyProxyChannel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let slot = self.slot(project_id);
        let channel = slot.get_or_dial(&self.dialer, project_id).await?;
        let status = match rpc(Arc::clone(&channel)).await {
            Ok(value) => return Ok(value),
            Err(status) => status,
        };
        if classify_status(&status) != StatusClass::StaleSession {
            return Err(status.into());
        }

        warn!(
            project_id,
            code = ?status.code(),
            message = status.message(),
            "gitaly proxy rejected a new stream as stale; rotating channel and retrying once"
        );
        slot.invalidate(&channel);
        let fresh = slot.get_or_dial(&self.dialer, project_id).await?;
        match rpc(fresh).await {
            Ok(value) => Ok(value),
            Err(status) if classify_status(&status) == StatusClass::StaleSession => {
                Err(GitalyProxyError::StaleAfterRetry(status))
            }
            Err(status) => Err(status.into()),
        }
    }
}

#[cfg(test)]
pub(super) fn connect_for_test(
    dialer: Arc<Dialer>,
    project_id: i64,
) -> impl Future<Output = Result<GitalyProxyChannel, GitalyProxyError>> {
    GitalyProxyChannel::connect(dialer, project_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(expires_in_secs: u64) -> Session {
        Session {
            repository: Repository::default(),
            profile: "readonly_repository".into(),
            expires_in: Duration::from_secs(expires_in_secs),
            connected_at: Instant::now(),
        }
    }

    #[test]
    fn rotates_at_ninety_percent_with_a_five_second_floor() {
        let s = session(600);
        assert_eq!(rotate_at(&s), s.connected_at + Duration::from_secs(540));

        let s = session(30);
        assert_eq!(rotate_at(&s), s.connected_at + Duration::from_secs(25));

        let s = session(40);
        assert_eq!(rotate_at(&s), s.connected_at + Duration::from_secs(35));

        // Below the floor the channel is single-use rather than never usable.
        let s = session(2);
        assert_eq!(rotate_at(&s), s.connected_at);
    }
}
