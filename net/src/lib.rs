use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use futures::{channel::mpsc, Stream, StreamExt};
use std::{
    convert::From,
    fmt::Debug,
    marker::{PhantomData, Send},
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;
use tracing::instrument;

pub mod channel;
pub mod quic;
pub mod tcp;

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("decoding error: {}", .0)]
    Decoding(String),
    #[error("connection error")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("connection error")]
    TcpConnection(#[from] std::io::Error),
    #[error("connection error")]
    MuxConnection(#[from] yamux::ConnectionError),
    #[error("stream error")]
    Stream(#[from] s2n_quic::stream::Error),
    #[error("unknown server error: {}", .0)]
    UnknownMsg(String),
    #[error("unknown server error")]
    Unknown,
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("connection error")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("initialization error: {}", .0)]
    Initialization(String),
    #[error("transport error")]
    Transport(#[from] TransportError),
    #[error("unknown client error")]
    Unknown,
}

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("transport error")]
    Decoding(#[from] TransportError),
    #[error("unknown server error")]
    Unknown,
    #[error("initialization error: {}", .0)]
    Initialization(String),
    #[error("unknown server error: {}", .0)]
    UnknownWithMessage(String),
    #[error("response error: {}", .0)]
    Response(String),
}

struct ProtoReader {
    buf: BytesMut,
}

impl ProtoReader {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(4096),
        }
    }

    pub fn add_data(&mut self, data: Bytes) {
        // Seems Hacky:
        // Problem is, the bytes _are_ remaining in the buffer, however they haven't been written
        // in from `data` yet
        let old_len = self.buf.len();
        self.buf.extend_from_slice(&data);
        self.buf.truncate(old_len + data.len());
    }

    #[instrument(skip(self))]
    pub fn read_message(&mut self) -> Option<Bytes> {
        let mut bytes_to_read: usize = 4;
        let mut size_data = self.buf.clone();

        if size_data.remaining() < 4 {
            return None;
        }

        let num_segments = size_data.get_u32_le() + 1;

        for _ in 0..num_segments {
            bytes_to_read += (size_data.get_u32_le() * 8) as usize;
        }

        if bytes_to_read % 8 != 0 {
            bytes_to_read += 4;
        }

        if bytes_to_read > self.buf.remaining() {
            return None;
        }

        let data = self.buf.split_to(bytes_to_read);
        Some(data.freeze())
    }
}

pub struct DataStream<E>
where
    E: Into<TransportError>,
{
    stream: Box<dyn Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static>,
    proto_reader: ProtoReader,
}

impl<E> DataStream<E>
where
    TransportError: From<E>,
{
    pub fn new(stream: impl Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static) -> Self {
        Self {
            stream: Box::new(stream),
            proto_reader: ProtoReader::new(),
        }
    }
}

impl<E> Stream for DataStream<E>
where
    TransportError: From<E>,
{
    type Item = Result<Bytes, TransportError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match futures::ready!(self.stream.poll_next_unpin(cx)) {
            Some(Ok(data)) => {
                self.proto_reader.add_data(data);
                match self.proto_reader.read_message() {
                    Some(bytes) => Poll::Ready(Some(Ok(bytes))),
                    None => self.poll_next(cx),
                }
            }
            Some(Err(e)) => {
                log::warn!("Stream: Error");
                Poll::Ready(Some(Err(e.into())))
            }
            None => Poll::Ready(None),
        }
    }
}

pub trait Decode {
    fn decode(bytes: &[u8]) -> Result<Self, TransportError>
    where
        Self: Sized;
}

pub trait Encode {
    fn encode(&self) -> Bytes;
}

pub struct MessageStream<'a, D, E>
where
    TransportError: From<E>,
{
    data_stream: DataStream<E>,
    phantom: PhantomData<&'a D>,
}

impl<'a, D, E> MessageStream<'a, D, E>
where
    TransportError: From<E>,
{
    pub fn new(data_stream: DataStream<E>) -> Self {
        Self {
            data_stream,
            phantom: PhantomData,
        }
    }
}

impl<'a, D, E> Stream for MessageStream<'a, D, E>
where
    D: Decode,
    TransportError: From<E>,
{
    type Item = Result<(D, Bytes), TransportError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match futures::ready!(self.data_stream.poll_next_unpin(cx)) {
            Some(Ok(data)) => match D::decode(data.as_ref()) {
                Ok(req) => Poll::Ready(Some(Ok((req, data)))),
                Err(e) => Poll::Ready(Some(Err(e.into()))),
            },
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

/// A inert network client. The client shouldn't contain any connections
#[async_trait]
pub trait Client<Req, Res>: Send + Sync {
    async fn connection(&self) -> Result<Box<dyn Connection<Req, Res>>, TransportError>;
    fn box_clone(&self) -> Box<dyn Client<Req, Res>>;
}

/// A network connection. The connection should represent a single connection to a remote server
/// and be able to create multiple streams over that connection.
#[async_trait]
pub trait Connection<Req, Res>: Send + Sync {
    async fn stream(&self) -> Result<Box<dyn MessageClient<Req, Res>>, TransportError>;
    fn box_clone(&self) -> Box<dyn Connection<Req, Res>>;
}

/// A stream over a connection. The stream can send multiple sequential request/responses
#[async_trait]
pub trait MessageClient<Req, Res>: Send + Sync + Debug {
    async fn request(&mut self, req: Req) -> Result<Res, TransportError>;
    fn box_clone(&self) -> Box<dyn MessageClient<Req, Res>>;
}

#[async_trait]
pub trait Server<Req, Res> {
    async fn run(&mut self) -> Result<(), ServerError>;
    fn port(&self) -> u16;
}

#[async_trait]
pub trait Handler<Req, Res>: Send + Sync + 'static {
    async fn call(&mut self, req: Req, send_tx: mpsc::Sender<Res>) -> Result<(), ServerError>;
    fn box_clone(&self) -> Box<dyn Handler<Req, Res>>;
}

#[derive(Debug, Clone)]
pub struct RequestWithMetadata<Req> {
    pub request_id: uhlc::Timestamp,
    pub request: Req,
}

impl<Req> RequestWithMetadata<Req> {
    pub fn new(request_id: uhlc::Timestamp, request: Req) -> Self {
        Self {
            request_id,
            request,
        }
    }
}
