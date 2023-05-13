use bytes::{Buf, BufMut, Bytes};
use capnp::serialize;
use net::{Decode, Encode, TransportError};
use std::io::Write;
use thiserror::Error;
use tokio::io;
use tokio::sync::oneshot;
use tracing::instrument;

use crate::server_capnp;

#[derive(Error, Debug)]
pub enum KVServerError {
    #[error("data store disconnected")]
    Disconnect(#[from] io::Error),
    #[error("channel error")]
    ChannelRecv(#[from] oneshot::error::RecvError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerRequest {
    Get {
        request_id: uhlc::Timestamp,
        key: String,
    },
    Digest {
        request_id: uhlc::Timestamp,
        key: String,
    },
    Put {
        request_id: uhlc::Timestamp,
        key: String,
        value: String,
    },
}

impl ServerRequest {
    pub fn key(&self) -> &String {
        match self {
            ServerRequest::Get { key, .. } => key,
            ServerRequest::Digest { key, .. } => key,
            ServerRequest::Put { key, .. } => key,
        }
    }

    pub fn request_id(&self) -> &uhlc::Timestamp {
        match self {
            ServerRequest::Get { request_id, .. } => request_id,
            ServerRequest::Digest { request_id, .. } => request_id,
            ServerRequest::Put { request_id, .. } => request_id,
        }
    }
}

impl Encode for ServerRequest {
    #[instrument]
    fn encode(&self) -> Bytes {
        let mut message = ::capnp::message::Builder::new_default();

        let mut res = message.init_root::<server_capnp::request::Builder>();

        match self {
            ServerRequest::Get { request_id, key } => {
                res.set_request_id(&request_id.to_string());
                let mut get = res.init_get();
                get.set_key(&key);
            }
            ServerRequest::Digest { request_id, key } => {
                res.set_request_id(&request_id.to_string());
                let mut digest = res.init_digest();
                digest.set_key(&key);
            }
            ServerRequest::Put {
                request_id,
                key,
                value,
            } => {
                res.set_request_id(&request_id.to_string());
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

impl Decode for ServerRequest {
    #[instrument(skip(buf))]
    fn decode(buf: &[u8]) -> Result<Self, TransportError> {
        let message_reader =
            serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

        let request = message_reader
            .get_root::<server_capnp::request::Reader>()
            .unwrap();

        let request_id = request
            .get_request_id()
            .map_err(|_e| TransportError::Unknown)?
            .parse()
            .map_err(|_e| TransportError::Unknown)?;

        match request.which().map_err(|_e| TransportError::Unknown)? {
            server_capnp::request::Which::Get(get_request) => {
                let key = get_request
                    .get_key()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                Ok(ServerRequest::Get { request_id, key })
            }
            server_capnp::request::Which::Digest(digest_request) => {
                let key = digest_request
                    .get_key()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                Ok(ServerRequest::Digest { request_id, key })
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

                Ok(ServerRequest::Put {
                    request_id,
                    key,
                    value,
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServerResponse {
    Error {
        request_id: uhlc::Timestamp,
        error: String,
    },
    Result {
        request_id: uhlc::Timestamp,
        data_id: Option<uhlc::Timestamp>,
        digest: Option<u64>,
        result: Option<String>,
    },
    Ok {
        request_id: uhlc::Timestamp,
    },
}

impl Encode for ServerResponse {
    #[instrument]
    fn encode(&self) -> Bytes {
        let mut message = ::capnp::message::Builder::new_default();

        let mut res = message.init_root::<server_capnp::response::Builder>();

        match self {
            ServerResponse::Error { request_id, error } => {
                res.set_request_id(&request_id.to_string());
                res.init_error().set_message(&error);
            }
            ServerResponse::Result {
                request_id,
                digest: Some(digest),
                data_id: Some(data_id),
                result: Some(data),
            } => {
                res.set_request_id(&request_id.to_string());
                let mut result = res.init_result();
                result.set_value(&data);
                result.set_data_id(&data_id.to_string());
                result.set_digest(*digest);
            }
            ServerResponse::Result {
                request_id,
                digest: None,
                data_id: None,
                result: None,
            } => {
                res.set_request_id(&request_id.to_string());
                res.init_result();
            }
            ServerResponse::Result { .. } => unreachable!(),
            ServerResponse::Ok { request_id } => {
                res.set_request_id(&request_id.to_string());
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

impl Decode for ServerResponse {
    #[instrument(skip(buf))]
    fn decode(buf: &[u8]) -> Result<Self, TransportError> {
        let message_reader =
            serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

        let response = message_reader
            .get_root::<server_capnp::response::Reader>()
            .unwrap();

        let request_id = response
            .get_request_id()
            .map_err(|_e| TransportError::Unknown)?
            .parse()
            .map_err(|_e| TransportError::Unknown)?;

        match response.which().map_err(|_e| TransportError::Unknown)? {
            server_capnp::response::Which::Result(result) => {
                let value = result
                    .get_value()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                let data_id = result
                    .get_data_id()
                    .map_err(|_e| TransportError::Unknown)?
                    .parse()
                    .map_err(|_e| TransportError::Unknown)?;

                let digest = result.get_digest();

                // Null Result?
                Ok(ServerResponse::Result {
                    request_id,
                    digest: Some(digest),
                    data_id: Some(data_id),
                    result: Some(value),
                })
            }
            server_capnp::response::Which::Ok(_) => Ok(ServerResponse::Ok { request_id }),
            server_capnp::response::Which::Error(result) => {
                let error = result
                    .get_message()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();
                Ok(ServerResponse::Error { request_id, error })
            }
        }
    }
}
