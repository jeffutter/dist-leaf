use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use futures::StreamExt;
use s2n_quic::{connection::Handle, stream::ReceiveStream};
use std::{
    convert::From,
    fmt::Debug,
    marker::{PhantomData, Send},
    pin::Pin,
    sync::atomic::AtomicU64,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::instrument;

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("decoding error: {}", .0)]
    Decoding(String),
    #[error("connection error")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("stream error")]
    Stream(#[from] s2n_quic::stream::Error),
    #[error("unknown server error: {}", .0)]
    UnknownMsg(String),
    #[error("unknown server error")]
    Unknown,
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

pub struct DataStream {
    stream: ReceiveStream,
    proto_reader: ProtoReader,
}

impl DataStream {
    pub fn new(stream: ReceiveStream) -> Self {
        Self {
            stream,
            proto_reader: ProtoReader::new(),
        }
    }
}

impl futures::stream::Stream for DataStream {
    type Item = Result<Bytes, TransportError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match futures::ready!(self.stream.poll_receive(cx)) {
            Ok(Some(data)) => {
                self.proto_reader.add_data(data);
                match self.proto_reader.read_message() {
                    Some(bytes) => Poll::Ready(Some(Ok(bytes))),
                    None => self.poll_next(cx),
                }
            }
            Ok(None) => Poll::Ready(None),
            Err(e) => {
                log::warn!("Stream: Error");
                Poll::Ready(Some(Err(e.into())))
            }
        }
    }
}

pub trait Decode {
    fn decode(bytes: &[u8]) -> Result<Self, TransportError>
    where
        Self: Sized;
    fn request_id(&self) -> &u64;
}

pub trait Encode {
    fn encode(&self) -> Bytes;
}

pub struct MessageStream<'a, D> {
    data_stream: DataStream,
    phantom: PhantomData<&'a D>,
}

impl<'a, D> MessageStream<'a, D> {
    pub fn new(data_stream: DataStream) -> Self {
        Self {
            data_stream,
            phantom: PhantomData,
        }
    }
}

impl<'a, D> futures::stream::Stream for MessageStream<'a, D>
where
    D: Decode,
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

impl<'a, D> From<ReceiveStream> for MessageStream<'a, D> {
    fn from(stream: ReceiveStream) -> Self {
        Self::new(DataStream::new(stream))
    }
}

#[async_trait]
pub trait MessageClient<Req, Res>: Send + Sync + Debug {
    async fn request(&mut self, req: Req) -> Result<Res, TransportError>;
    fn box_clone(&self) -> Box<dyn MessageClient<Req, Res>>;
}

pub struct QuicMessageClient<Req, ReqT, Res, ResT> {
    request_counter: AtomicU64,
    handle: Handle,
    phantom1: PhantomData<Req>,
    phantom2: PhantomData<Res>,
    phantom3: PhantomData<ReqT>,
    phantom4: PhantomData<ResT>,
}

impl<Req, ReqT, Res, ResT> QuicMessageClient<Req, ReqT, Res, ResT>
where
    ReqT: Encode + Decode + Debug + From<RequestWithMetadata<Req>>,
    ResT: Encode + Decode + Debug + Sync + Send + 'static,
{
    pub async fn new(connection: Handle) -> Result<Self, TransportError> {
        Ok(Self {
            handle: connection,
            request_counter: AtomicU64::new(0),
            phantom1: PhantomData,
            phantom2: PhantomData,
            phantom3: PhantomData,
            phantom4: PhantomData,
        })
    }
}

#[async_trait]
impl<Req, ReqT, Res, ResT> MessageClient<Req, Res> for QuicMessageClient<Req, ReqT, Res, ResT>
where
    ReqT: Encode + Decode + From<RequestWithMetadata<Req>> + Send + Sync + Debug + 'static,
    ResT: Encode + Decode + Debug + Send + Sync + Debug + 'static,
    Res: From<ResT> + Send + Sync + Debug + 'static,
    Req: Send + Sync + Debug + 'static,
{
    #[instrument(skip(self), fields(message_client = "Quic"))]
    async fn request(&mut self, req: Req) -> Result<Res, TransportError> {
        let stream = self.handle.open_bidirectional_stream().await?;
        let (receive_stream, mut send_stream) = stream.split();

        let id = self
            .request_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let kvrt: ReqT = RequestWithMetadata::new(id, req).into();
        let encoded = kvrt.encode();
        send_stream.send(encoded).await.unwrap();
        let mut response_stream: MessageStream<ResT> = receive_stream.into();
        let (result, _data) = response_stream
            .next()
            .await
            .expect("stream should be open")?;

        Ok(result.into())
    }

    fn box_clone(&self) -> Box<dyn MessageClient<Req, Res>> {
        Box::new(QuicMessageClient {
            handle: self.handle.clone(),
            request_counter: AtomicU64::new(0),
            phantom1: PhantomData,
            phantom2: PhantomData,
            phantom3: PhantomData::<ReqT>,
            phantom4: PhantomData::<ResT>,
        })
    }
}

impl<Req, ReqT, Res, ResT> Debug for QuicMessageClient<Req, ReqT, Res, ResT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicMessageClient")
            .field("socket", &self.handle.remote_addr().unwrap())
            .finish()
    }
}

pub struct ChannelMessageClient<Req, Res> {
    server: mpsc::Sender<(Req, oneshot::Sender<Res>)>,
}

impl<Req, Res> Clone for ChannelMessageClient<Req, Res> {
    fn clone(&self) -> ChannelMessageClient<Req, Res> {
        ChannelMessageClient {
            server: self.server.clone(),
        }
    }
}

impl<Req, Res> ChannelMessageClient<Req, Res>
where
    Req: Debug,
    Res: Debug,
{
    pub fn new(server: mpsc::Sender<(Req, oneshot::Sender<Res>)>) -> Self {
        Self { server }
    }
}

#[async_trait]
impl<Req, Res> MessageClient<Req, Res> for ChannelMessageClient<Req, Res>
where
    Req: Debug + Send + 'static,
    Res: Debug + Send + 'static,
{
    #[instrument(skip(self), fields(message_client = "Channel"))]
    async fn request(&mut self, req: Req) -> Result<Res, TransportError> {
        let (tx, rx) = oneshot::channel();
        self.server
            .send((req, tx))
            .await
            .expect("channel should be open");
        let result = rx.await.expect("channel should be open");

        Ok(result)
    }

    fn box_clone(&self) -> Box<dyn MessageClient<Req, Res>> {
        Box::new(ChannelMessageClient {
            server: self.server.clone(),
        })
    }
}

impl<Req, Res> Debug for ChannelMessageClient<Req, Res> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelMessageClient").finish()
    }
}

pub struct RequestWithMetadata<Req> {
    pub request_id: u64,
    pub request: Req,
}

impl<Req> RequestWithMetadata<Req> {
    pub fn new(request_id: u64, request: Req) -> Self {
        Self {
            request_id,
            request,
        }
    }
}
