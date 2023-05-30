use futures::{AsyncRead, AsyncWrite};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead as TokioAsyncRead;
use tokio::io::AsyncWrite as TokioAsyncWrite;
use tokio::io::ReadBuf;

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

pub fn from_tokio<Z>(adapted: Z) -> impl Stream
where
    Z: TokioAsyncRead + TokioAsyncWrite + Unpin + Send + 'static,
{
    FromAdapter { adapted }
}

struct FromAdapter<Z> {
    adapted: Z,
}

impl<Z: TokioAsyncRead + Unpin> AsyncRead for FromAdapter<Z> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let before = buf.len();
        let x = Pin::new(&mut self.get_mut().adapted).poll_read(cx, &mut ReadBuf::new(buf));
        x.map(|x| x.map(|()| buf.len() - before))
    }
}

impl<Z: TokioAsyncWrite + Unpin> AsyncWrite for FromAdapter<Z> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.get_mut().adapted).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().adapted).poll_flush(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().adapted).poll_shutdown(cx)
    }
}

impl<Z: TokioAsyncRead + TokioAsyncWrite + Unpin + Send + 'static> Stream for FromAdapter<Z> {}

pub fn to_tokio<S: Stream>(adapted: S) -> TokioStream<S> {
    TokioStream { adapted }
}

pub struct TokioStream<S> {
    adapted: S,
}

impl<S: Stream> TokioAsyncRead for TokioStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().adapted)
            .poll_read(cx, buf.initialized_mut())
            .map(|x| x.map(|_| ()))
    }
}

impl<S: Stream> TokioAsyncWrite for TokioStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.get_mut().adapted).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().adapted).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().adapted).poll_close(cx)
    }
}
