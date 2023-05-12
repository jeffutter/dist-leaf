use bytes::{Buf, BufMut, Bytes};
use capnp::serialize;
use net::{Decode, Encode, TransportError};
use std::io::Write;
use tracing::instrument;

use crate::client_capnp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientRequest {
    Get {
        request_id: uhlc::Timestamp,
        key: String,
    },
    Put {
        request_id: uhlc::Timestamp,
        key: String,
        value: String,
    },
}

impl ClientRequest {
    pub fn key(&self) -> &String {
        match self {
            ClientRequest::Get { key, .. } => key,
            ClientRequest::Put { key, .. } => key,
        }
    }

    pub fn request_id(&self) -> &uhlc::Timestamp {
        match self {
            ClientRequest::Get { request_id, .. } => request_id,
            ClientRequest::Put { request_id, .. } => request_id,
        }
    }
}

impl Encode for ClientRequest {
    #[instrument]
    fn encode(&self) -> Bytes {
        let mut message = ::capnp::message::Builder::new_default();

        let mut res = message.init_root::<client_capnp::request::Builder>();

        match self {
            ClientRequest::Get { request_id, key } => {
                res.set_request_id(&request_id.to_string());
                let mut get = res.init_get();
                get.set_key(&key);
            }
            ClientRequest::Put {
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

impl Decode for ClientRequest {
    #[instrument(skip(buf))]
    fn decode(buf: &[u8]) -> Result<Self, TransportError> {
        let message_reader =
            serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

        let request = message_reader
            .get_root::<client_capnp::request::Reader>()
            .unwrap();

        let request_id = request
            .get_request_id()
            .map_err(|_e| TransportError::Unknown)?
            .parse()
            .map_err(|_e| TransportError::Unknown)?;

        match request.which().map_err(|_e| TransportError::Unknown)? {
            client_capnp::request::Which::Get(get_request) => {
                let key = get_request
                    .get_key()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                Ok(ClientRequest::Get { request_id, key })
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

                Ok(ClientRequest::Put {
                    request_id,
                    key,
                    value,
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClientResponse {
    Error {
        request_id: uhlc::Timestamp,
        error: String,
    },
    Result {
        request_id: uhlc::Timestamp,
        result: Option<String>,
    },
    Ok(uhlc::Timestamp),
}

impl Encode for ClientResponse {
    #[instrument]
    fn encode(&self) -> Bytes {
        let mut message = ::capnp::message::Builder::new_default();

        let mut res = message.init_root::<client_capnp::response::Builder>();

        match self {
            ClientResponse::Error { request_id, error } => {
                res.set_request_id(&request_id.to_string());
                res.init_error().set_message(&error);
            }
            ClientResponse::Result {
                request_id,
                result: Some(x),
            } => {
                res.set_request_id(&request_id.to_string());
                res.init_result().set_value(&x);
            }
            ClientResponse::Result {
                request_id,
                result: None,
            } => {
                res.set_request_id(&request_id.to_string());
                res.init_result();
            }
            ClientResponse::Ok(request_id) => {
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

impl Decode for ClientResponse {
    #[instrument(skip(buf))]
    fn decode(buf: &[u8]) -> Result<Self, TransportError> {
        let message_reader =
            serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

        let response = message_reader
            .get_root::<client_capnp::response::Reader>()
            .unwrap();

        let request_id = response
            .get_request_id()
            .map_err(|_e| TransportError::Unknown)?
            .parse()
            .map_err(|_e| TransportError::Unknown)?;

        match response.which().map_err(|_e| TransportError::Unknown)? {
            client_capnp::response::Which::Result(result) => {
                let value = result
                    .get_value()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                // Null Result?
                Ok(ClientResponse::Result {
                    request_id,
                    result: Some(value),
                })
            }
            client_capnp::response::Which::Ok(_) => Ok(ClientResponse::Ok(request_id)),
            client_capnp::response::Which::Error(result) => {
                let error = result
                    .get_message()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();
                Ok(ClientResponse::Error { request_id, error })
            }
        }
    }
}
