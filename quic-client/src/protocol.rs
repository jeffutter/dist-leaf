use bytes::{Buf, BufMut, Bytes};
use capnp::serialize;
use quic_transport::{Decode, Encode, RequestWithMetadata, TransportError};
use std::io::Write;
use tracing::instrument;

use crate::client_capnp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientRequest {
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

impl ClientRequest {
    pub fn key(&self) -> &String {
        match self {
            ClientRequest::Get { key, .. } => key,
            ClientRequest::Put { key, .. } => key,
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
                res.set_request_id(*request_id);
                let mut get = res.init_get();
                get.set_key(&key);
            }
            ClientRequest::Put {
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

impl Decode for ClientRequest {
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

    fn request_id(&self) -> &u64 {
        match self {
            ClientRequest::Get { request_id, .. } => request_id,
            ClientRequest::Put { request_id, .. } => request_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClientResponse {
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

impl Encode for ClientResponse {
    #[instrument]
    fn encode(&self) -> Bytes {
        let mut message = ::capnp::message::Builder::new_default();

        let mut res = message.init_root::<client_capnp::response::Builder>();

        match self {
            ClientResponse::Error { request_id, error } => {
                res.set_request_id(*request_id);
                res.init_error().set_message(&error);
            }
            ClientResponse::Result {
                request_id,
                result: Some(x),
            } => {
                res.set_request_id(*request_id);
                res.init_result().set_value(&x);
            }
            ClientResponse::Result {
                request_id,
                result: None,
            } => {
                res.set_request_id(*request_id);
                res.init_result();
            }
            ClientResponse::Ok(request_id) => {
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

impl Decode for ClientResponse {
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

    fn request_id(&self) -> &u64 {
        match self {
            ClientResponse::Error { request_id, .. } => request_id,
            ClientResponse::Result { request_id, .. } => request_id,
            ClientResponse::Ok(request_id) => request_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientCommand {
    Get { key: String },
    Put { key: String, value: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientCommandResponse {
    Error { error: String },
    Result { result: Option<String> },
    Ok,
}

impl From<RequestWithMetadata<ClientRequest>> for ClientRequest {
    fn from(req_with_id: RequestWithMetadata<ClientRequest>) -> Self {
        req_with_id.request
    }
}

impl From<RequestWithMetadata<ClientResponse>> for ClientResponse {
    fn from(res_with_id: RequestWithMetadata<ClientResponse>) -> Self {
        res_with_id.request
    }
}

impl From<ClientResponse> for ClientCommandResponse {
    fn from(response: ClientResponse) -> Self {
        match response {
            ClientResponse::Error { error, .. } => ClientCommandResponse::Error { error },
            ClientResponse::Result { result, .. } => ClientCommandResponse::Result { result },
            ClientResponse::Ok(_) => ClientCommandResponse::Ok,
        }
    }
}

impl From<RequestWithMetadata<ClientCommand>> for ClientRequest {
    fn from(req_with_id: RequestWithMetadata<ClientCommand>) -> Self {
        match req_with_id.request {
            ClientCommand::Get { key } => ClientRequest::Get {
                request_id: req_with_id.request_id,
                key,
            },
            ClientCommand::Put { key, value } => ClientRequest::Put {
                request_id: req_with_id.request_id,
                key,
                value,
            },
        }
    }
}

impl From<RequestWithMetadata<ClientCommandResponse>> for ClientResponse {
    fn from(res_with_id: RequestWithMetadata<ClientCommandResponse>) -> Self {
        match res_with_id.request {
            ClientCommandResponse::Error { error } => ClientResponse::Error {
                request_id: res_with_id.request_id,
                error,
            },
            ClientCommandResponse::Result { result } => ClientResponse::Result {
                request_id: res_with_id.request_id,
                result,
            },
            ClientCommandResponse::Ok => ClientResponse::Ok(res_with_id.request_id),
        }
    }
}
