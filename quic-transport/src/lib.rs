use bytes::Bytes;
use futures::StreamExt;
use net::{decode_request, decode_response, KVRequestType, KVResponseType, ProtoReader};
use s2n_quic::stream::ReceiveStream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("decoding error")]
    Decoding(#[from] net::KVServerError),
    #[error("connection error")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("stream error")]
    Stream(#[from] s2n_quic::stream::Error),
    #[error("unknown server error")]
    Unknown,
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

pub struct RequestStream {
    data_stream: DataStream,
}

impl RequestStream {
    pub fn new(data_stream: DataStream) -> Self {
        Self { data_stream }
    }
}

impl futures::stream::Stream for RequestStream {
    type Item = Result<(KVRequestType, Bytes), TransportError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match futures::ready!(self.data_stream.poll_next_unpin(cx)) {
            Some(Ok(data)) => match decode_request(data.as_ref()) {
                Ok(req) => Poll::Ready(Some(Ok((req, data)))),
                Err(e) => Poll::Ready(Some(Err(e.into()))),
            },
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl From<ReceiveStream> for RequestStream {
    fn from(stream: ReceiveStream) -> Self {
        Self::new(DataStream::new(stream))
    }
}

pub struct ResponseStream {
    data_stream: DataStream,
}

impl ResponseStream {
    pub fn new(data_stream: DataStream) -> Self {
        Self { data_stream }
    }
}

impl futures::stream::Stream for ResponseStream {
    type Item = Result<(KVResponseType, Bytes), TransportError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match futures::ready!(self.data_stream.poll_next_unpin(cx)) {
            Some(Ok(data)) => match decode_response(data.as_ref()) {
                Ok(req) => Poll::Ready(Some(Ok((req, data)))),
                Err(e) => Poll::Ready(Some(Err(e.into()))),
            },
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl From<ReceiveStream> for ResponseStream {
    fn from(stream: ReceiveStream) -> Self {
        Self::new(DataStream::new(stream))
    }
}
