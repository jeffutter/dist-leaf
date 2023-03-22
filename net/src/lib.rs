use bytes::{Buf, BufMut, Bytes, BytesMut};
use capnp::serialize;
use std::io::Write;
use thiserror::Error;
use tokio::io;
use tokio::sync::oneshot;

pub mod net_capnp {
    include!(concat!(env!("OUT_DIR"), "/net_capnp.rs"));
}

pub struct ProtoReader {
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

pub fn encode_request(request: KVRequestType) -> Bytes {
    let mut message = ::capnp::message::Builder::new_default();

    let res = message.init_root::<net_capnp::request::Builder>();

    match request {
        KVRequestType::Get(key) => {
            res.init_get().set_key(&key);
        }
        KVRequestType::Put(key, value) => {
            let mut put = res.init_put();
            put.set_key(&key);
            put.set_value(&value);
        }
    }

    let mut buf = vec![];
    {
        let reference = buf.by_ref();
        let writer = reference.writer();
        serialize::write_message(writer, &message).unwrap();
    }

    buf.into()
}

pub fn encode_response(response: KVResponseType) -> Bytes {
    let mut message = ::capnp::message::Builder::new_default();

    let res = message.init_root::<net_capnp::response::Builder>();

    match response {
        KVResponseType::Error(e) => {
            res.init_error().set_message(&e);
        }
        KVResponseType::Result(Some(x)) => {
            res.init_result().set_value(&x);
        }
        KVResponseType::Result(None) => {
            res.init_result();
        }
        KVResponseType::Ok => {
            res.init_ok();
        }
    }

    let mut buf = vec![];
    {
        let reference = buf.by_ref();
        let writer = reference.writer();
        serialize::write_message(writer, &message).unwrap();
    }

    buf.into()
}

pub fn decode_request(buf: &[u8]) -> Result<KVRequestType, KVServerError> {
    let message_reader =
        serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

    let request = message_reader
        .get_root::<net_capnp::request::Reader>()
        .unwrap();

    match request.which().map_err(|_e| KVServerError::Unknown)? {
        net_capnp::request::Which::Get(get_request) => {
            let key = get_request
                .get_key()
                .map_err(|_e| KVServerError::Unknown)?
                .to_string();

            Ok(KVRequestType::Get(key))
        }
        net_capnp::request::Which::Put(put_request) => {
            let key = put_request
                .get_key()
                .map_err(|_e| KVServerError::Unknown)?
                .to_string();

            let value = put_request
                .get_value()
                .map_err(|_e| KVServerError::Unknown)?
                .to_string();

            Ok(KVRequestType::Put(key, value))
        }
    }
}

pub fn decode_response(buf: &[u8]) -> Result<KVResponseType, KVServerError> {
    let message_reader =
        serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

    let response = message_reader
        .get_root::<net_capnp::response::Reader>()
        .unwrap();

    match response.which().map_err(|_e| KVServerError::Unknown)? {
        net_capnp::response::Which::Result(result) => {
            let value = result
                .get_value()
                .map_err(|_e| KVServerError::Unknown)?
                .to_string();

            // Null Result?
            Ok(KVResponseType::Result(Some(value)))
        }
        net_capnp::response::Which::Ok(_) => Ok(KVResponseType::Ok),
        net_capnp::response::Which::Error(result) => {
            let error = result
                .get_message()
                .map_err(|_e| KVServerError::Unknown)?
                .to_string();
            Ok(KVResponseType::Error(error))
        }
    }
}

#[derive(Error, Debug)]
pub enum KVServerError {
    #[error("data store disconnected")]
    Disconnect(#[from] io::Error),
    #[error("channel error")]
    ChannelRecv(#[from] oneshot::error::RecvError),
    #[error("unknown kv store error")]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KVRequestType {
    Get(String),
    Put(String, String),
}

impl KVRequestType {
    pub fn key(&self) -> &String {
        match self {
            KVRequestType::Get(key) => key,
            KVRequestType::Put(key, _) => key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KVResponseType {
    Error(String),
    Result(Option<String>),
    Ok,
}
