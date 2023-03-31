use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use futures::StreamExt;
use s2n_quic::{
    stream::{ReceiveStream, SendStream},
    Connection,
};
use std::{
    collections::HashMap,
    convert::From,
    fmt::Debug,
    marker::{PhantomData, Send},
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    time::Instant,
};

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("decoding error: {}", .0)]
    Decoding(String),
    #[error("connection error")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("stream error")]
    Stream(#[from] s2n_quic::stream::Error),
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

    pub fn read_message(&mut self) -> Option<Bytes> {
        let mut bytes_to_read: usize = 4;
        let mut size_data = self.buf.clone();

        if size_data.remaining() < 4 {
            return None;
        }

        let num_segments = size_data.get_u32_le() + 1;
        log::debug!("Segments: {}", num_segments);

        for _ in 0..num_segments {
            bytes_to_read += (size_data.get_u32_le() * 8) as usize;
        }

        if bytes_to_read % 8 != 0 {
            bytes_to_read += 4;
        }

        log::debug!("{} > {}", bytes_to_read, self.buf.remaining());

        if bytes_to_read > self.buf.remaining() {
            log::debug!("Full Message not In Buffer");
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
                log::debug!("Stream: Error");
                Poll::Ready(Some(Err(e.into())))
            }
        }
    }
}

pub trait Decode {
    type Item;
    fn decode(bytes: &[u8]) -> Result<Self::Item, TransportError>;
    fn id(&self) -> &u64;
}

pub trait Encode {
    type Item;
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
    type Item = Result<(<D as Decode>::Item, Bytes), TransportError>;

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
}

#[derive(Debug)]
pub struct QuicMessageClient<Req, ReqT, Res, ResT> {
    pending_requests: Arc<Mutex<HashMap<u64, oneshot::Sender<ResT>>>>,
    request_counter: AtomicU64,
    send_stream: SendStream,
    phantom1: PhantomData<Req>,
    phantom2: PhantomData<Res>,
    phantom3: PhantomData<ReqT>,
}

impl<Req, ReqT, Res, ResT> QuicMessageClient<Req, ReqT, Res, ResT>
where
    ReqT: Encode + Decode + Debug + From<RequestWithId<Req>>,
    ResT: Encode + Decode<Item = ResT> + Debug + Sync + Send + 'static,
{
    pub async fn new(connection: Arc<Mutex<Connection>>) -> Result<Self, TransportError> {
        // open a new stream and split the receiving and sending sides
        let stream = connection.lock().await.open_bidirectional_stream().await?;
        let (receive_stream, send_stream) = stream.split();

        let pending_requests: Arc<Mutex<HashMap<u64, oneshot::Sender<ResT>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let pending_requests1 = pending_requests.clone();
        tokio::spawn(async move {
            let mut response_stream: MessageStream<ResT> = receive_stream.into();
            while let Some(Ok((req, _data))) = response_stream.next().await {
                let tx = pending_requests1.lock().await.remove(req.id()).unwrap();
                tx.send(req).unwrap();
            }
        });

        Ok(Self {
            send_stream,
            pending_requests,

            request_counter: AtomicU64::new(0),
            phantom1: PhantomData,
            phantom2: PhantomData,
            phantom3: PhantomData,
        })
    }
}

#[async_trait]
impl<Req, ReqT, Res, ResT> MessageClient<Req, Res> for QuicMessageClient<Req, ReqT, Res, ResT>
where
    ReqT: Encode + Decode<Item = ReqT> + From<RequestWithId<Req>> + Send + Sync + Debug,
    ResT: Encode + Decode<Item = ResT> + Debug + Send + Sync + Debug + 'static,
    Res: From<ResT> + Send + Sync + Debug,
    Req: Send + Sync + Debug,
{
    async fn request(&mut self, req: Req) -> Result<Res, TransportError> {
        let start = Instant::now();
        let id = self
            .request_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let kvrt: ReqT = RequestWithId::new(id, req).into();
        let encoded = kvrt.encode();
        log::debug!("Encoded Data: {:?}", encoded);
        log::debug!(
            "encoded: {}µs",
            Instant::now().duration_since(start).as_micros()
        );
        self.send_stream.send(encoded).await.unwrap();
        let (tx, rx) = oneshot::channel();
        self.pending_requests.lock().await.insert(id, tx);
        log::debug!(
            "sent: {}µs",
            Instant::now().duration_since(start).as_micros()
        );
        let result = rx.await.expect("channel should be open");
        log::debug!(
            "received: {}µs",
            Instant::now().duration_since(start).as_micros()
        );
        Ok(result.into())
    }
}

#[derive(Clone, Debug)]
pub struct ChannelMessageClient<Req, Res> {
    server: mpsc::Sender<(Req, oneshot::Sender<Res>)>,
}

impl<Req, Res> ChannelMessageClient<Req, Res>
where
    Req: std::fmt::Debug,
    Res: std::fmt::Debug,
{
    pub fn new(server: mpsc::Sender<(Req, oneshot::Sender<Res>)>) -> Self {
        Self { server }
    }
}

#[async_trait]
impl<Req, Res> MessageClient<Req, Res> for ChannelMessageClient<Req, Res>
where
    Req: Debug + Send,
    Res: Debug + Send,
{
    async fn request(&mut self, req: Req) -> Result<Res, TransportError> {
        let (tx, rx) = oneshot::channel();
        self.server
            .send((req, tx))
            .await
            .expect("channel should be open");
        let result = rx.await.expect("channel should be open");

        Ok(result)
    }
}

pub struct RequestWithId<Req> {
    pub id: u64,
    pub request: Req,
}

impl<Req> RequestWithId<Req> {
    pub fn new(id: u64, request: Req) -> Self {
        Self { id, request }
    }
}
