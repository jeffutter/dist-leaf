use bytes::{Buf, BufMut, Bytes};
use capnp::serialize;
use quic_transport::{Decode, Encode, RequestWithId, TransportError};
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
    #[instrument]
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
    #[instrument(skip(buf))]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KVResponse {
    Error { id: u64, error: String },
    Result { id: u64, result: Option<String> },
    Ok(u64),
}

impl Encode for KVResponse {
    #[instrument]
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
    #[instrument(skip(buf))]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KVReq {
    Get { key: String },
    Put { key: String, value: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KVRes {
    Error { error: String },
    Result { result: Option<String> },
    Ok,
}

impl From<KVRequest> for KVReq {
    fn from(req: KVRequest) -> Self {
        match req {
            KVRequest::Get { key, .. } => KVReq::Get { key },
            KVRequest::Put { key, value, .. } => KVReq::Put { key, value },
        }
    }
}

impl From<KVResponse> for KVRes {
    fn from(res: KVResponse) -> Self {
        match res {
            KVResponse::Error { error, .. } => KVRes::Error { error },
            KVResponse::Result { result, .. } => KVRes::Result { result },
            KVResponse::Ok(_) => KVRes::Ok,
        }
    }
}

impl From<RequestWithId<KVReq>> for KVRequest {
    fn from(req_with_id: RequestWithId<KVReq>) -> Self {
        match req_with_id.request {
            KVReq::Get { key } => KVRequest::Get {
                id: req_with_id.id,
                key,
            },
            KVReq::Put { key, value } => KVRequest::Put {
                id: req_with_id.id,
                key,
                value,
            },
        }
    }
}

impl From<RequestWithId<KVRes>> for KVResponse {
    fn from(res_with_id: RequestWithId<KVRes>) -> Self {
        match res_with_id.request {
            KVRes::Error { error } => KVResponse::Error {
                id: res_with_id.id,
                error,
            },
            KVRes::Result { result } => KVResponse::Result {
                id: res_with_id.id,
                result,
            },
            KVRes::Ok => KVResponse::Ok(res_with_id.id),
        }
    }
}
