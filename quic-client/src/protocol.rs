use bytes::{Buf, BufMut, Bytes};
use capnp::serialize;
use quic_transport::{Decode, Encode, RequestWithMetadata, TransportError};
use std::io::Write;
use tracing::instrument;

use crate::client_capnp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KVRequestType {
    Get {
        request_id: u64,
        key: String,
    },
    Put {
        request_id: u64,
        key: String,
        value: String,
    },
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
            KVRequestType::Get { request_id, key } => {
                res.set_request_id(*request_id);
                let mut get = res.init_get();
                get.set_key(&key);
            }
            KVRequestType::Put {
                request_id,
                key,
                value,
            } => {
                res.set_request_id(*request_id);
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

        let request_id = request.get_request_id();

        match request.which().map_err(|_e| TransportError::Unknown)? {
            client_capnp::request::Which::Get(get_request) => {
                let key = get_request
                    .get_key()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                Ok(KVRequestType::Get { request_id, key })
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

                Ok(KVRequestType::Put {
                    request_id,
                    key,
                    value,
                })
            }
        }
    }

    fn request_id(&self) -> &u64 {
        match self {
            KVRequestType::Get { request_id, .. } => request_id,
            KVRequestType::Put { request_id, .. } => request_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KVResponseType {
    Error {
        request_id: u64,
        error: String,
    },
    Result {
        request_id: u64,
        result: Option<String>,
    },
    Ok(u64),
}

impl Encode for KVResponseType {
    #[instrument]
    fn encode(&self) -> Bytes {
        let mut message = ::capnp::message::Builder::new_default();

        let mut res = message.init_root::<client_capnp::response::Builder>();

        match self {
            KVResponseType::Error { request_id, error } => {
                res.set_request_id(*request_id);
                res.init_error().set_message(&error);
            }
            KVResponseType::Result {
                request_id,
                result: Some(x),
            } => {
                res.set_request_id(*request_id);
                res.init_result().set_value(&x);
            }
            KVResponseType::Result {
                request_id,
                result: None,
            } => {
                res.set_request_id(*request_id);
                res.init_result();
            }
            KVResponseType::Ok(request_id) => {
                res.set_request_id(*request_id);
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

        let request_id = response.get_request_id();

        match response.which().map_err(|_e| TransportError::Unknown)? {
            client_capnp::response::Which::Result(result) => {
                let value = result
                    .get_value()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                // Null Result?
                Ok(KVResponseType::Result {
                    request_id,
                    result: Some(value),
                })
            }
            client_capnp::response::Which::Ok(_) => Ok(KVResponseType::Ok(request_id)),
            client_capnp::response::Which::Error(result) => {
                let error = result
                    .get_message()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();
                Ok(KVResponseType::Error { request_id, error })
            }
        }
    }

    fn request_id(&self) -> &u64 {
        match self {
            KVResponseType::Error { request_id, .. } => request_id,
            KVResponseType::Result { request_id, .. } => request_id,
            KVResponseType::Ok(request_id) => request_id,
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
                request_id: req_with_id.request_id,
                key,
            },
            KVRequest::Put { key, value } => KVRequestType::Put {
                request_id: req_with_id.request_id,
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
                request_id: res_with_id.request_id,
                error,
            },
            KVResponse::Result { result } => KVResponseType::Result {
                request_id: res_with_id.request_id,
                result,
            },
            KVResponse::Ok => KVResponseType::Ok(res_with_id.request_id),
        }
    }
}
