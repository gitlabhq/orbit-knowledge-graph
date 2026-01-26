use tokio_util::sync::CancellationToken;

/// Waits for SIGINT or SIGTERM, then cancels the token.
pub async fn wait_for_signal(shutdown: CancellationToken) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");

    shutdown.cancel();
}
