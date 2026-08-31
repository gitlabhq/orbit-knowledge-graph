use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Sink, Stream};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

pub struct WebSocketIo {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pending_read: Bytes,
}

impl WebSocketIo {
    async fn connect(url: &str) -> io::Result<Self> {
        let (socket, _) = connect_async(url).await.map_err(ws_error)?;
        Ok(Self {
            socket,
            pending_read: Bytes::new(),
        })
    }
}

impl AsyncRead for WebSocketIo {
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

            match Pin::new(&mut self.socket).poll_next(cx) {
                Poll::Ready(Some(Ok(Message::Binary(data)))) => self.pending_read = data,
                Poll::Ready(Some(Ok(Message::Close(_)))) | Poll::Ready(None) => {
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Ok(Message::Ping(data)))) => {
                    match Pin::new(&mut self.socket).poll_ready(cx) {
                        Poll::Ready(Ok(())) => Pin::new(&mut self.socket)
                            .start_send(Message::Pong(data))
                            .map_err(ws_error)?,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(ws_error(error))),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                Poll::Ready(Some(Ok(_))) => {}
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(ws_error(error))),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncWrite for WebSocketIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
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

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.socket).poll_flush(cx).map_err(ws_error)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.socket).poll_close(cx).map_err(ws_error)
    }
}

pub async fn connect_channel(url: impl Into<String>) -> Result<Channel, tonic::transport::Error> {
    let url = url.into();
    Endpoint::from_static("http://workhorse.internal")
        .connect_with_connector(service_fn(move |_| {
            let url = url.clone();
            async move { WebSocketIo::connect(&url).await.map(TokioIo::new) }
        }))
        .await
}

fn ws_error(error: Error) -> io::Error {
    io::Error::other(error)
}
