use bytes::{Buf, BufMut, Bytes};
use capnp::serialize;
use quic_transport::{Decode, Encode, TransportError};
use std::io::Write;
use thiserror::Error;
use tokio::io;
use tokio::sync::oneshot;

use crate::server_capnp;

#[derive(Error, Debug)]
pub enum KVServerError {
    #[error("data store disconnected")]
    Disconnect(#[from] io::Error),
    #[error("channel error")]
    ChannelRecv(#[from] oneshot::error::RecvError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KVRequest {
    Get { id: u64, key: String },
    Put { id: u64, key: String, value: String },
}

impl KVRequest {
    pub fn key(&self) -> &String {
        match self {
            KVRequest::Get { key, .. } => key,
            KVRequest::Put { key, .. } => key,
        }
    }
}

impl Encode for KVRequest {
    type Item = Self;

    fn encode(&self) -> Bytes {
        let mut message = ::capnp::message::Builder::new_default();

        let mut res = message.init_root::<server_capnp::request::Builder>();

        match self {
            KVRequest::Get { id, key } => {
                res.set_id(*id);
                let mut get = res.init_get();
                get.set_key(&key);
            }
            KVRequest::Put { id, key, value } => {
                res.set_id(*id);
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
}

impl Decode for KVRequest {
    type Item = Self;

    fn decode(buf: &[u8]) -> Result<Self, TransportError> {
        let message_reader =
            serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

        let request = message_reader
            .get_root::<server_capnp::request::Reader>()
            .unwrap();

        let id = request.get_id();

        match request.which().map_err(|_e| TransportError::Unknown)? {
            server_capnp::request::Which::Get(get_request) => {
                let key = get_request
                    .get_key()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                Ok(KVRequest::Get { id, key })
            }
            server_capnp::request::Which::Put(put_request) => {
                let key = put_request
                    .get_key()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                let value = put_request
                    .get_value()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                Ok(KVRequest::Put { id, key, value })
            }
        }
    }

    fn id(&self) -> &u64 {
        match self {
            KVRequest::Get { id, .. } => id,
            KVRequest::Put { id, .. } => id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KVResponse {
    Error { id: u64, error: String },
    Result { id: u64, result: Option<String> },
    Ok(u64),
}

impl Encode for KVResponse {
    type Item = Self;

    fn encode(&self) -> Bytes {
        let mut message = ::capnp::message::Builder::new_default();

        let mut res = message.init_root::<server_capnp::response::Builder>();

        match self {
            KVResponse::Error { id, error } => {
                res.set_id(*id);
                res.init_error().set_message(&error);
            }
            KVResponse::Result {
                id,
                result: Some(x),
            } => {
                res.set_id(*id);
                res.init_result().set_value(&x);
            }
            KVResponse::Result { id, result: None } => {
                res.set_id(*id);
                res.init_result();
            }
            KVResponse::Ok(id) => {
                res.set_id(*id);
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
}

impl Decode for KVResponse {
    type Item = Self;

    fn decode(buf: &[u8]) -> Result<Self, TransportError> {
        let message_reader =
            serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

        let response = message_reader
            .get_root::<server_capnp::response::Reader>()
            .unwrap();

        let id = response.get_id();

        match response.which().map_err(|_e| TransportError::Unknown)? {
            server_capnp::response::Which::Result(result) => {
                let value = result
                    .get_value()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                // Null Result?
                Ok(KVResponse::Result {
                    id,
                    result: Some(value),
                })
            }
            server_capnp::response::Which::Ok(_) => Ok(KVResponse::Ok(id)),
            server_capnp::response::Which::Error(result) => {
                let error = result
                    .get_message()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();
                Ok(KVResponse::Error { id, error })
            }
        }
    }

    fn id(&self) -> &u64 {
        match self {
            KVResponse::Error { id, .. } => id,
            KVResponse::Result { id, .. } => id,
            KVResponse::Ok(id) => id,
        }
    }
}
