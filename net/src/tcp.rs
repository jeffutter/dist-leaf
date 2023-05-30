use std::fmt::Debug;
use std::pin::Pin;
use std::task::Poll;
use std::{format, future};
use std::{marker::PhantomData, net::SocketAddr};

use async_trait::async_trait;
use bytes::Bytes;
use futures::channel::mpsc;
use futures::{stream, AsyncRead, AsyncReadExt, AsyncWriteExt, StreamExt, TryStreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{event, instrument, Level};
use yamux::Stream;

use crate::{
    Client, ClientError, Connection, DataStream, Decode, Encode, MessageClient, MessageStream,
    TransportError,
};
use crate::{Handler, Server, ServerError};

#[derive(Clone)]
pub struct TcpClient<Req, Res> {
    addr: SocketAddr,
    req: PhantomData<Req>,
    res: PhantomData<Res>,
}

impl<Req, Res> TcpClient<Req, Res>
where
    Req: Encode + Decode + Debug + Send + Clone,
    Res: Encode + Decode + Debug + Sync + Send + Clone + 'static,
{
    pub fn new(addr: SocketAddr) -> Result<Self, ClientError> {
        Ok(Self {
            addr,
            req: PhantomData,
            res: PhantomData,
        })
    }
}

#[async_trait]
impl<Req, Res> Client<Req, Res> for TcpClient<Req, Res>
where
    Req: Encode + Decode + Debug + Sync + Send + Clone + 'static,
    Res: Encode + Decode + Debug + Sync + Send + Clone + 'static,
{
    #[instrument(skip(self), fields(addr))]
    async fn connection(&self) -> Result<Box<dyn Connection<Req, Res>>, TransportError> {
        event!(Level::INFO, "Connecting to {:?}", self.addr);
        let stream = TcpStream::connect(self.addr).await?;
        event!(Level::INFO, "Creating TcpConnection {:?}", self.addr);
        let conn = TcpConnection::new(stream).await;

        Ok(Box::new(conn))
    }

    fn box_clone(&self) -> Box<dyn Client<Req, Res>> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
pub struct TcpConnection<Req, Res> {
    control: yamux::Control,
    req: PhantomData<Req>,
    res: PhantomData<Res>,
}

impl<Req, Res> TcpConnection<Req, Res>
where
    Req: Encode + Decode + Debug + Send,
    Res: Encode + Decode + Debug + Sync + Send + 'static,
{
    #[instrument]
    pub async fn new(stream: TcpStream) -> Self {
        let stream_debug = format!("{:?}", stream);
        event!(Level::INFO, "Muxing {}", stream_debug);
        let muxed = yamux::Connection::new(
            stream.compat(),
            yamux::Config::default(),
            yamux::Mode::Client,
        );
        event!(Level::INFO, "Muxed {}", stream_debug);

        event!(Level::INFO, "Controlling {}", stream_debug);
        let (control, client) = yamux::Control::new(muxed);
        event!(Level::INFO, "Controlled {}", stream_debug);

        tokio::spawn(async move {
            client
                .for_each(|maybe_stream| {
                    drop(maybe_stream);
                    future::ready(())
                })
                .await;
        });

        Self {
            control,
            req: PhantomData,
            res: PhantomData,
        }
    }
}

pub struct TcpMessageClient<Req, Res> {
    control: yamux::Control,
    phantom1: PhantomData<Req>,
    phantom2: PhantomData<Res>,
}

impl<Req, Res> TcpMessageClient<Req, Res>
where
    Req: Encode + Decode + Debug,
    Res: Encode + Decode + Debug + Sync + Send + 'static,
{
    pub fn new(control: yamux::Control) -> Self {
        Self {
            control,
            phantom1: PhantomData,
            phantom2: PhantomData,
        }
    }
}

#[async_trait]
impl<Req, Res> Connection<Req, Res> for TcpConnection<Req, Res>
where
    Req: Encode + Decode + Debug + Sync + Send + 'static,
    Res: Encode + Decode + Debug + Sync + Send + 'static,
{
    async fn stream(&self) -> Result<Box<dyn MessageClient<Req, Res>>, TransportError> {
        Ok(Box::new(TcpMessageClient::new(self.control.clone())))
    }

    fn box_clone(&self) -> Box<dyn Connection<Req, Res>> {
        Box::new(TcpConnection {
            control: self.control.clone(),
            req: PhantomData,
            res: PhantomData,
        })
    }
}

#[async_trait]
impl<Req, Res> MessageClient<Req, Res> for TcpMessageClient<Req, Res>
where
    Req: Encode + Decode + Send + Sync + Debug + 'static,
    Res: Encode + Decode + Send + Sync + Debug + 'static,
{
    #[instrument(skip(self), fields(message_client = "Tcp"))]
    async fn request(&mut self, req: Req) -> Result<Res, TransportError> {
        let mut stream = self.control.open_stream().await.unwrap();

        let encoded = req.encode();
        stream.write_all(&encoded).await.unwrap();
        let data_stream = DataStream::new(
            stream.map(|x| x.map(|packet| Bytes::copy_from_slice(packet.as_ref()))),
        );
        let mut response_stream = MessageStream::<Res, _>::new(data_stream);
        let (result, _data) = response_stream
            .next()
            .await
            .expect("stream should be open")?;

        Ok(result.into())
    }

    fn box_clone(&self) -> Box<dyn MessageClient<Req, Res>> {
        Box::new(TcpMessageClient {
            control: self.control.clone(),
            phantom1: PhantomData,
            phantom2: PhantomData,
        })
    }
}

impl<Req, Res> Debug for TcpMessageClient<Req, Res> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpMessageClient")
            .field("socket", &self.control)
            .finish()
    }
}

const BUFFER_SIZE: usize = 4096;

struct ReaderStream<R> {
    reader: R,
    buffer: Vec<u8>,
    buffer_pos: usize,
}

impl<R: AsyncRead> ReaderStream<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: Vec::with_capacity(BUFFER_SIZE),
            buffer_pos: 0,
        }
    }
}

impl<R: AsyncRead + Unpin> futures::Stream for ReaderStream<R> {
    type Item = Result<Bytes, TransportError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        if this.buffer_pos < this.buffer.len() {
            let start_pos = this.buffer_pos;
            let end_pos = this.buffer.len();
            this.buffer_pos = end_pos;
            return Poll::Ready(Some(Ok(this.buffer[start_pos..end_pos].to_owned().into())));
        }

        this.buffer_pos = 0;
        this.buffer.resize(BUFFER_SIZE.into(), 0);

        match Pin::new(&mut this.reader).poll_read(cx, &mut this.buffer) {
            Poll::Ready(Ok(0)) => Poll::Ready(None),
            Poll::Ready(Ok(read_bytes)) => {
                this.buffer.truncate(read_bytes);
                let start_pos = this.buffer_pos;
                let end_pos = this.buffer.len();
                this.buffer_pos = end_pos;
                Poll::Ready(Some(Ok(this.buffer[start_pos..end_pos].to_owned().into())))
            }
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => {
                Poll::Ready(Some(Err(TransportError::UnknownMsg(e.to_string()))))
            }
        }
    }
}

pub struct TcpServer<Req, Res> {
    handler: Box<dyn Handler<Req, Res>>,
    listener: TcpListener,
    pub port: u16,
    phantom_data: PhantomData<Res>,
}

impl<Req, Res> TcpServer<Req, Res>
where
    Req: Encode + Decode + Send + Sync + Debug + 'static,
    Res: Encode + Decode + Send + Sync + Debug + 'static,
{
    pub async fn new(handler: impl Handler<Req, Res>) -> Result<Self, ServerError> {
        let listener = TcpListener::bind("0.0.0.0:0")
            .await
            .map_err(|e| ServerError::Initialization(e.to_string()))?;

        let port = listener
            .local_addr()
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .port();

        Ok(Self {
            listener,
            port,
            handler: Box::new(handler),
            phantom_data: PhantomData,
        })
    }

    #[instrument(skip(handler))]
    async fn handle_connection(
        stream: TcpStream,
        handler: Box<dyn Handler<Req, Res>>,
    ) -> Result<(), ServerError> {
        let stream_debug = format!("{:?}", stream);
        event!(Level::INFO, "Muxing {}", stream_debug);
        let mut muxed = yamux::Connection::new(
            stream.compat(),
            yamux::Config::default(),
            yamux::Mode::Server,
        );
        event!(Level::INFO, "Muxed {}", stream_debug);

        stream::poll_fn(|cx| muxed.poll_next_inbound(cx))
            .try_for_each_concurrent(None, |stream| {
                event!(Level::INFO, "Received Stream {}", stream_debug);
                let handler = handler.box_clone();
                async move {
                    Self::handle_stream(stream, handler).await;
                    Ok(())
                }
            })
            .await
            .map_err(|e| ServerError::UnknownWithMessage(e.to_string()))
    }

    #[instrument(skip(handler))]
    async fn handle_stream(stream: Stream, handler: Box<dyn Handler<Req, Res>>) {
        let (read_stream, mut write_stream) = AsyncReadExt::split(stream);
        let data_stream = DataStream::new(ReaderStream::new(read_stream));
        let mut request_stream = MessageStream::<Req, _>::new(data_stream);

        let (send_tx, mut send_rx): (mpsc::Sender<Res>, mpsc::Receiver<Res>) = mpsc::channel(100);

        tokio::spawn(async move {
            while let Some(data) = send_rx.next().await {
                let encoded = data.encode();
                write_stream
                    .write_all(&encoded)
                    .await
                    .map_err(|e| ServerError::UnknownWithMessage(e.to_string()))?;
            }

            Ok::<(), ServerError>(())
        });

        while let Some(Ok((req, _data))) = request_stream.next().await {
            let send_tx = send_tx.clone();
            let mut handler = handler.box_clone();
            tokio::spawn(async move { handler.call(req, send_tx).await });
        }
    }
}

#[async_trait]
impl<Req, Res> Server<Req, Res> for TcpServer<Req, Res>
where
    Req: Encode + Decode + Send + Sync + Debug + 'static,
    Res: Encode + Decode + Send + Sync + Debug + 'static,
{
    async fn run(&mut self) -> Result<(), ServerError> {
        while let Ok((socket, _addr)) = self.listener.accept().await {
            let handler = self.handler.box_clone();
            tokio::spawn(Self::handle_connection(socket, handler));
        }

        Ok(())
    }

    fn port(&self) -> u16 {
        self.port
    }
}
