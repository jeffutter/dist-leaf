use bytes::{Buf, BufMut, Bytes};
use capnp::serialize;
use quic_transport::{Decode, Encode, RequestWithMetadata, TransportError};
use std::io::Write;
use tracing::instrument;

use crate::client_capnp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KVRequestType {
    Get { id: u64, key: String },
    Put { id: u64, key: String, value: String },
}

impl KVRequestType {
    pub fn key(&self) -> &String {
        match self {
            KVRequestType::Get { key, .. } => key,
            KVRequestType::Put { key, .. } => key,
        }
    }
}

impl Encode for KVRequestType {
    #[instrument]
    fn encode(&self) -> Bytes {
        let mut message = ::capnp::message::Builder::new_default();

        let mut res = message.init_root::<client_capnp::request::Builder>();

        match self {
            KVRequestType::Get { id, key } => {
                res.set_id(*id);
                let mut get = res.init_get();
                get.set_key(&key);
            }
            KVRequestType::Put { id, key, value } => {
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

impl Decode for KVRequestType {
    #[instrument(skip(buf))]
    fn decode(buf: &[u8]) -> Result<Self, TransportError> {
        let message_reader =
            serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

        let request = message_reader
            .get_root::<client_capnp::request::Reader>()
            .unwrap();

        let id = request.get_id();

        match request.which().map_err(|_e| TransportError::Unknown)? {
            client_capnp::request::Which::Get(get_request) => {
                let key = get_request
                    .get_key()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                Ok(KVRequestType::Get { id, key })
            }
            client_capnp::request::Which::Put(put_request) => {
                let key = put_request
                    .get_key()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                let value = put_request
                    .get_value()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                Ok(KVRequestType::Put { id, key, value })
            }
        }
    }

    fn id(&self) -> &u64 {
        match self {
            KVRequestType::Get { id, .. } => id,
            KVRequestType::Put { id, .. } => id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KVResponseType {
    Error { id: u64, error: String },
    Result { id: u64, result: Option<String> },
    Ok(u64),
}

impl Encode for KVResponseType {
    #[instrument]
    fn encode(&self) -> Bytes {
        let mut message = ::capnp::message::Builder::new_default();

        let mut res = message.init_root::<client_capnp::response::Builder>();

        match self {
            KVResponseType::Error { id, error } => {
                res.set_id(*id);
                res.init_error().set_message(&error);
            }
            KVResponseType::Result {
                id,
                result: Some(x),
            } => {
                res.set_id(*id);
                res.init_result().set_value(&x);
            }
            KVResponseType::Result { id, result: None } => {
                res.set_id(*id);
                res.init_result();
            }
            KVResponseType::Ok(id) => {
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

impl Decode for KVResponseType {
    #[instrument(skip(buf))]
    fn decode(buf: &[u8]) -> Result<Self, TransportError> {
        let message_reader =
            serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

        let response = message_reader
            .get_root::<client_capnp::response::Reader>()
            .unwrap();

        let id = response.get_id();

        match response.which().map_err(|_e| TransportError::Unknown)? {
            client_capnp::response::Which::Result(result) => {
                let value = result
                    .get_value()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                // Null Result?
                Ok(KVResponseType::Result {
                    id,
                    result: Some(value),
                })
            }
            client_capnp::response::Which::Ok(_) => Ok(KVResponseType::Ok(id)),
            client_capnp::response::Which::Error(result) => {
                let error = result
                    .get_message()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();
                Ok(KVResponseType::Error { id, error })
            }
        }
    }

    fn id(&self) -> &u64 {
        match self {
            KVResponseType::Error { id, .. } => id,
            KVResponseType::Result { id, .. } => id,
            KVResponseType::Ok(id) => id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KVRequest {
    Get { key: String },
    Put { key: String, value: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KVResponse {
    Error { error: String },
    Result { result: Option<String> },
    Ok,
}

impl From<RequestWithMetadata<KVRequestType>> for KVRequestType {
    fn from(req_with_id: RequestWithMetadata<KVRequestType>) -> Self {
        req_with_id.request
    }
}

impl From<RequestWithMetadata<KVResponseType>> for KVResponseType {
    fn from(res_with_id: RequestWithMetadata<KVResponseType>) -> Self {
        res_with_id.request
    }
}

impl From<KVResponseType> for KVResponse {
    fn from(response: KVResponseType) -> Self {
        match response {
            KVResponseType::Error { error, .. } => KVResponse::Error { error },
            KVResponseType::Result { result, .. } => KVResponse::Result { result },
            KVResponseType::Ok(_) => KVResponse::Ok,
        }
    }
}

impl From<RequestWithMetadata<KVRequest>> for KVRequestType {
    fn from(req_with_id: RequestWithMetadata<KVRequest>) -> Self {
        match req_with_id.request {
            KVRequest::Get { key } => KVRequestType::Get {
                id: req_with_id.id,
                key,
            },
            KVRequest::Put { key, value } => KVRequestType::Put {
                id: req_with_id.id,
                key,
                value,
            },
        }
    }
}

impl From<RequestWithMetadata<KVResponse>> for KVResponseType {
    fn from(res_with_id: RequestWithMetadata<KVResponse>) -> Self {
        match res_with_id.request {
            KVResponse::Error { error } => KVResponseType::Error {
                id: res_with_id.id,
                error,
            },
            KVResponse::Result { result } => KVResponseType::Result {
                id: res_with_id.id,
                result,
            },
            KVResponse::Ok => KVResponseType::Ok(res_with_id.id),
        }
    }
}
