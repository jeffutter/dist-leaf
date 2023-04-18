use bytes::{Buf, BufMut, Bytes};
use capnp::serialize;
use quic_client::protocol::ClientRequest;
use quic_transport::{Decode, Encode, RequestWithMetadata, TransportError};
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
        request_id: u64,
        key: String,
    },
    Put {
        request_id: u64,
        key: String,
        value: String,
    },
}

impl ServerRequest {
    pub fn key(&self) -> &String {
        match self {
            ServerRequest::Get { key, .. } => key,
            ServerRequest::Put { key, .. } => key,
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
                res.set_request_id(*request_id);
                let mut get = res.init_get();
                get.set_key(&key);
            }
            ServerRequest::Put {
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

impl Decode for ServerRequest {
    #[instrument(skip(buf))]
    fn decode(buf: &[u8]) -> Result<Self, TransportError> {
        let message_reader =
            serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

        let request = message_reader
            .get_root::<server_capnp::request::Reader>()
            .unwrap();

        let request_id = request.get_request_id();

        match request.which().map_err(|_e| TransportError::Unknown)? {
            server_capnp::request::Which::Get(get_request) => {
                let key = get_request
                    .get_key()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                Ok(ServerRequest::Get { request_id, key })
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

    fn request_id(&self) -> &u64 {
        match self {
            ServerRequest::Get { request_id, .. } => request_id,
            ServerRequest::Put { request_id, .. } => request_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServerResponse {
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

impl Encode for ServerResponse {
    #[instrument]
    fn encode(&self) -> Bytes {
        let mut message = ::capnp::message::Builder::new_default();

        let mut res = message.init_root::<server_capnp::response::Builder>();

        match self {
            ServerResponse::Error { request_id, error } => {
                res.set_request_id(*request_id);
                res.init_error().set_message(&error);
            }
            ServerResponse::Result {
                request_id,
                result: Some(x),
            } => {
                res.set_request_id(*request_id);
                res.init_result().set_value(&x);
            }
            ServerResponse::Result {
                request_id,
                result: None,
            } => {
                res.set_request_id(*request_id);
                res.init_result();
            }
            ServerResponse::Ok(request_id) => {
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

impl Decode for ServerResponse {
    #[instrument(skip(buf))]
    fn decode(buf: &[u8]) -> Result<Self, TransportError> {
        let message_reader =
            serialize::read_message(buf.reader(), ::capnp::message::ReaderOptions::new()).unwrap();

        let response = message_reader
            .get_root::<server_capnp::response::Reader>()
            .unwrap();

        let request_id = response.get_request_id();

        match response.which().map_err(|_e| TransportError::Unknown)? {
            server_capnp::response::Which::Result(result) => {
                let value = result
                    .get_value()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();

                // Null Result?
                Ok(ServerResponse::Result {
                    request_id,
                    result: Some(value),
                })
            }
            server_capnp::response::Which::Ok(_) => Ok(ServerResponse::Ok(request_id)),
            server_capnp::response::Which::Error(result) => {
                let error = result
                    .get_message()
                    .map_err(|_e| TransportError::Unknown)?
                    .to_string();
                Ok(ServerResponse::Error { request_id, error })
            }
        }
    }

    fn request_id(&self) -> &u64 {
        match self {
            ServerResponse::Error { request_id, .. } => request_id,
            ServerResponse::Result { request_id, .. } => request_id,
            ServerResponse::Ok(request_id) => request_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerCommand {
    Get { key: String },
    Put { key: String, value: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerCommandResponse {
    Error { error: String },
    Result { result: Option<String> },
    Ok,
}

impl From<ServerRequest> for ServerCommand {
    fn from(req: ServerRequest) -> Self {
        match req {
            ServerRequest::Get { key, .. } => ServerCommand::Get { key },
            ServerRequest::Put { key, value, .. } => ServerCommand::Put { key, value },
        }
    }
}

impl From<ClientRequest> for ServerCommand {
    fn from(req: ClientRequest) -> Self {
        match req {
            ClientRequest::Get { key, .. } => ServerCommand::Get { key },
            ClientRequest::Put { key, value, .. } => ServerCommand::Put { key, value },
        }
    }
}

impl From<ServerResponse> for ServerCommandResponse {
    fn from(res: ServerResponse) -> Self {
        match res {
            ServerResponse::Error { error, .. } => ServerCommandResponse::Error { error },
            ServerResponse::Result { result, .. } => ServerCommandResponse::Result { result },
            ServerResponse::Ok(_) => ServerCommandResponse::Ok,
        }
    }
}

impl From<RequestWithMetadata<ServerCommand>> for ServerRequest {
    fn from(req_with_id: RequestWithMetadata<ServerCommand>) -> Self {
        match req_with_id.request {
            ServerCommand::Get { key } => ServerRequest::Get {
                request_id: req_with_id.request_id,
                key,
            },
            ServerCommand::Put { key, value } => ServerRequest::Put {
                request_id: req_with_id.request_id,
                key,
                value,
            },
        }
    }
}

impl From<RequestWithMetadata<ServerCommandResponse>> for ServerResponse {
    fn from(res_with_id: RequestWithMetadata<ServerCommandResponse>) -> Self {
        match res_with_id.request {
            ServerCommandResponse::Error { error } => ServerResponse::Error {
                request_id: res_with_id.request_id,
                error,
            },
            ServerCommandResponse::Result { result } => ServerResponse::Result {
                request_id: res_with_id.request_id,
                result,
            },
            ServerCommandResponse::Ok => ServerResponse::Ok(res_with_id.request_id),
        }
    }
}
