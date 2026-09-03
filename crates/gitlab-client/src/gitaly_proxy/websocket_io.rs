use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as WsError, Message, Utf8Bytes};

/// `AsyncRead + AsyncWrite` over a WebSocket carrying a single HTTP/2
/// connection in binary messages: one `poll_write` is one binary message, one
/// binary message is drained by as many `poll_read`s as it takes.
///
/// # One-writer invariant
///
/// The h2 connection task is the only writer. Pongs are queued and flushed from
/// `poll_read`, which the same task drives, so nothing else may hold a writer
/// to the socket. This is a client-side adapter; do not wrap a server-accepted
/// socket in it outside test fixtures.
///
/// # Close mapping
///
/// Only a close frame with code 1000 or 1001 is a clean EOF. Any other close
/// code, a close without a code, or a socket that ends without a close
/// handshake is an `io::Error`, so a truncated gRPC stream surfaces as
/// `Unavailable` instead of a short but "successful" body.
///
/// Shutting the adapter down sends a 1000 close frame, so the peer applies the
/// same rule to us: `SinkExt::close` alone would send a close without a status
/// code, which the server logs as an abnormal end.
pub struct WebSocketIo<S> {
    socket: WebSocketStream<S>,
    pending_read: Bytes,
    pong_unflushed: bool,
    closed_cleanly: bool,
    close_sent: bool,
}

impl<S> WebSocketIo<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(socket: WebSocketStream<S>) -> Self {
        Self {
            socket,
            pending_read: Bytes::new(),
            pong_unflushed: false,
            closed_cleanly: false,
            close_sent: false,
        }
    }

    /// A pong queued behind a large DATA frame in the write buffer is how a
    /// healthy client gets closed for pong timeout, so it is flushed eagerly.
    fn poll_flush_pong(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.pong_unflushed {
            return Poll::Ready(Ok(()));
        }
        match Pin::new(&mut self.socket).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                self.pong_unflushed = false;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(ws_error(error))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn queue_pong(&mut self, cx: &mut Context<'_>, payload: Bytes) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.socket).poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                Pin::new(&mut self.socket)
                    .start_send(Message::Pong(payload))
                    .map_err(ws_error)?;
                self.pong_unflushed = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(ws_error(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> AsyncRead for WebSocketIo<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if !self.pending_read.is_empty() {
                let amount = buf.remaining().min(self.pending_read.len());
                buf.put_slice(&self.pending_read.split_to(amount));
                return Poll::Ready(Ok(()));
            }
            if self.closed_cleanly {
                return Poll::Ready(Ok(()));
            }
            if self.poll_flush_pong(cx)?.is_pending() {
                return Poll::Pending;
            }

            match Pin::new(&mut self.socket).poll_next(cx) {
                Poll::Ready(Some(Ok(Message::Binary(data)))) => self.pending_read = data,
                Poll::Ready(Some(Ok(Message::Ping(payload)))) => {
                    if self.queue_pong(cx, payload)?.is_pending() {
                        return Poll::Pending;
                    }
                }
                Poll::Ready(Some(Ok(Message::Close(frame)))) => {
                    return match frame {
                        Some(frame)
                            if matches!(frame.code, CloseCode::Normal | CloseCode::Away) =>
                        {
                            self.closed_cleanly = true;
                            Poll::Ready(Ok(()))
                        }
                        Some(frame) => Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            format!(
                                "websocket closed with code {}: {}",
                                u16::from(frame.code),
                                frame.reason
                            ),
                        ))),
                        None => Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "websocket closed without a status code",
                        ))),
                    };
                }
                Poll::Ready(Some(Ok(Message::Text(_) | Message::Pong(_) | Message::Frame(_)))) => {}
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(ws_error(error))),
                Poll::Ready(None) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "websocket ended without a close handshake",
                    )));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S> AsyncWrite for WebSocketIo<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.socket).poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                Pin::new(&mut self.socket)
                    .start_send(Message::Binary(Bytes::copy_from_slice(buf)))
                    .map_err(ws_error)?;
                Poll::Ready(Ok(buf.len()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(ws_error(error))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.socket).poll_flush(cx).map_err(ws_error);
        if let Poll::Ready(Ok(())) = result {
            self.pong_unflushed = false;
        }
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.close_sent {
            let Poll::Ready(ready) = Pin::new(&mut self.socket).poll_ready(cx) else {
                return Poll::Pending;
            };
            self.close_sent = true;
            // A peer that already closed makes both calls fail; poll_close
            // below still completes the handshake from our side.
            if ready.is_ok() {
                let _ = Pin::new(&mut self.socket).start_send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: Utf8Bytes::default(),
                })));
            }
        }
        Pin::new(&mut self.socket).poll_close(cx).map_err(ws_error)
    }
}

fn ws_error(error: WsError) -> io::Error {
    match error {
        WsError::Io(error) => error,
        WsError::ConnectionClosed | WsError::AlreadyClosed => {
            io::Error::new(io::ErrorKind::NotConnected, error)
        }
        WsError::Capacity(_) => io::Error::new(io::ErrorKind::InvalidData, error),
        other => io::Error::other(other),
    }
}
