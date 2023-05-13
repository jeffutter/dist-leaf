use crate::{
    node_registry::ConnectionRegistry,
    protocol::{ServerRequest, ServerResponse},
    vnode::{client_mdns::ClientMDNS, VNodeId},
};
use async_trait::async_trait;
use client::protocol::{ClientRequest, ClientResponse};
use futures::{channel::mpsc, SinkExt};
use net::{
    quic::{Handler, Server, ServerError},
    MessageClient, TransportError,
};
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc, usize};
use tokio::{select, spawn, sync::Mutex, task::JoinSet};
use tracing::{event, instrument, Level};
use uhlc::Timestamp;
use uuid::Uuid;

static REPLICATION_FACTOR: usize = 3;
static CONSISTENCY_LEVEL: usize = match (REPLICATION_FACTOR / 2, REPLICATION_FACTOR % 2) {
    (n, 0) => n,
    (n, _) => n + 1,
};

struct ClientHandler {
    hlc: Arc<Mutex<uhlc::HLC>>,
    connection_registry: ConnectionRegistry<ServerRequest, ServerResponse>,
}

impl ClientHandler {
    fn new(
        connection_registry: ConnectionRegistry<ServerRequest, ServerResponse>,
        hlc: Arc<Mutex<uhlc::HLC>>,
    ) -> Self {
        Self {
            connection_registry,
            hlc,
        }
    }
}

async fn put(
    req: ClientRequest,
    mut send_tx: mpsc::Sender<ClientResponse>,
    replicas: Vec<(
        VNodeId,
        Box<dyn MessageClient<ServerRequest, ServerResponse>>,
    )>,
    local_request_id: Timestamp,
) -> Result<(), ServerError> {
    let client_request_id = req.request_id().clone();
    let mut join_set: JoinSet<Result<(VNodeId, ServerResponse), ServerError>> = JoinSet::new();
    let server_request = match req {
        ClientRequest::Put { key, value, .. } => ServerRequest::Put {
            request_id: local_request_id,
            key,
            value,
        },
        _ => unreachable!(),
    };

    for (vnode_id, connection) in replicas.iter() {
        let server_request = server_request.clone();
        let vnode_id = vnode_id.clone();
        let mut connection = connection.box_clone();
        join_set.spawn({
            async move {
                let res = connection.request(server_request).await?;

                Ok::<_, ServerError>((vnode_id, res))
            }
        });
    }

    let mut results: HashMap<ServerResponse, (usize, Vec<VNodeId>)> = HashMap::new();
    let mut received_responses = 0;
    let mut result_sent = false;

    while let Some(res) = join_set.join_next().await {
        received_responses += 1;

        let r = match res {
            Ok(Ok((_, ServerResponse::Error { error, .. }))) => Err(ServerError::Response(error)),
            Ok(Ok(res)) => Ok(res),
            Ok(Err(e)) => Err(e),
            Err(_e) => Err(ServerError::Unknown),
        };

        match r {
            Ok((vnode_id, r)) => {
                results
                    .entry(r.clone())
                    .and_modify(|(i, vnodes)| {
                        *i += 1;
                        vnodes.push(vnode_id.clone());
                    })
                    .or_insert((1, vec![vnode_id]));

                if results[&r].0 >= CONSISTENCY_LEVEL {
                    let response = match &r {
                        ServerResponse::Ok { .. } => ClientResponse::Ok(client_request_id),
                        ServerResponse::Error { .. } => unreachable!(),
                        ServerResponse::Result { .. } => unreachable!(),
                    };

                    send_tx.send(response).await.expect("channel sould be open");

                    result_sent = true;

                    event!(
                        Level::INFO,
                        "Results matched with {} request(s)",
                        results[&r].0
                    );

                    break;
                }

                if received_responses >= REPLICATION_FACTOR {
                    let res = ClientResponse::Error {
                        request_id: client_request_id,
                        error: "Results did not match".to_string(),
                    };

                    send_tx.try_send(res).expect("channel should be open");
                    result_sent = true;

                    event!(Level::ERROR, results = ?results, "Results did not match");

                    break;
                }
            }
            Err(e) => {
                event!(Level::ERROR, error = ?e, "Error fetching from another server");
            }
        }
    }

    if result_sent == false {
        let res = ClientResponse::Error {
            request_id: client_request_id,
            error: "Could not fetch enough results".to_string(),
        };

        send_tx.send(res).await.expect("channel should be open");
    }

    // We want to let in-flight requests finish
    join_set.detach_all();
    Ok(())
}

async fn get(
    req: ClientRequest,
    mut send_tx: mpsc::Sender<ClientResponse>,
    replicas: Vec<(
        VNodeId,
        Box<dyn MessageClient<ServerRequest, ServerResponse>>,
    )>,
    local_request_id: Timestamp,
) -> Result<(), ServerError> {
    let client_request_id = req.request_id().clone();
    let req_key = req.key().clone();
    let mut join_set: JoinSet<Result<(VNodeId, ServerResponse), ServerError>> = JoinSet::new();
    let server_request = match req {
        ClientRequest::Get { key, .. } => ServerRequest::Get {
            request_id: local_request_id,
            key,
        },
        _ => unreachable!(),
    };

    let digest_request = ServerRequest::Digest {
        request_id: local_request_id,
        key: req_key,
    };

    //
    // TODO: Find "fastest" node to send "GET" to
    //

    let ((data_vnode_id, data_client), replicas) = replicas.split_first().unwrap();
    let data_vnode_id = data_vnode_id.clone();
    let mut data_client = data_client.box_clone();

    for (vnode_id, connection) in replicas.iter() {
        let vnode_id = vnode_id.clone();
        let digest_request = digest_request.clone();
        let mut connection = connection.box_clone();
        join_set.spawn({
            async move {
                let res = connection.request(digest_request).await?;

                Ok::<_, ServerError>((vnode_id, res))
            }
        });
    }

    let mut digests: HashMap<Option<u64>, (usize, Vec<VNodeId>)> = HashMap::new();
    let mut errors: Vec<VNodeId> = Vec::new();
    let mut response: Option<Result<ServerResponse, TransportError>> = None;
    let mut received_responses = 0usize;
    let mut result_sent = false;

    let data_vnode_id1 = data_vnode_id.clone();
    join_set.spawn({
        async move {
            let res = data_client.request(server_request).await?;
            Ok::<_, ServerError>((data_vnode_id1, res))
        }
    });

    while let Some(res) = join_set.join_next().await {
        received_responses += 1;

        let res = match res {
            Ok(Ok((_, ServerResponse::Error { error, .. }))) => Err(ServerError::Response(error)),
            Ok(Ok(res)) => Ok(res),
            Ok(Err(e)) => Err(e),
            Err(_e) => Err(ServerError::Unknown),
        };

        match res {
            Ok((vnode_id, res @ ServerResponse::Result { .. })) => {
                if vnode_id == data_vnode_id {
                    event!(Level::INFO, res = ?res, "Data Response");
                    response = Some(Ok(res));
                } else {
                    event!(Level::INFO, res = ?res, "Digest Response");
                    let ServerResponse::Result { digest, .. } = res else { unreachable!() };
                    digests
                        .entry(digest)
                        .and_modify(|(i, vnodes)| {
                            *i += 1;
                            vnodes.push(vnode_id.clone());
                        })
                        .or_insert((1, vec![vnode_id]));
                }
            }
            Ok((vnode_id, ServerResponse::Error { .. })) => {
                errors.push(vnode_id);
            }
            Ok((_vnode_id, ServerResponse::Ok { .. })) => unreachable!(),
            Err(_x) => {
                unimplemented!()
            }
        }

        if let Some(Ok(ServerResponse::Result { digest, .. })) = response {
            if digests.get(&digest).unwrap_or(&(0usize, Vec::new())).0 >= CONSISTENCY_LEVEL {
                let response = match response.unwrap().unwrap() {
                    ServerResponse::Result { result, .. } => ClientResponse::Result {
                        request_id: client_request_id,
                        result,
                    },
                    ServerResponse::Ok { .. } => unreachable!(),
                    ServerResponse::Error { .. } => unreachable!(),
                };

                send_tx.send(response).await.expect("channel sould be open");

                result_sent = true;

                break;
            }
        }

        if received_responses >= REPLICATION_FACTOR {
            let res = ClientResponse::Error {
                request_id: client_request_id,
                error: "Results did not match".to_string(),
            };

            send_tx.try_send(res).expect("channel should be open");
            result_sent = true;

            event!(Level::ERROR, response = ?response, digests =? digests, "Results did not match");

            break;
        }
    }

    if result_sent == false {
        let res = ClientResponse::Error {
            request_id: client_request_id,
            error: "Could not fetch enough results".to_string(),
        };

        send_tx.send(res).await.expect("channel should be open");
    }

    Ok(())
}

#[async_trait]
impl Handler<ClientRequest, ClientResponse> for ClientHandler {
    #[instrument(skip(self, send_tx))]
    async fn call(
        &mut self,
        req: ClientRequest,
        send_tx: mpsc::Sender<ClientResponse>,
    ) -> Result<(), ServerError> {
        let req_key = req.key().clone();
        let replicas = self
            .connection_registry
            .replicas(&req_key, REPLICATION_FACTOR)
            .await;

        event!(Level::INFO, "Fetching {:?} from: {:?}", req, replicas);

        let local_request_id = self.hlc.lock().await.new_timestamp();

        match req {
            ClientRequest::Put { .. } => put(req, send_tx, replicas, local_request_id).await,
            ClientRequest::Get { .. } => get(req, send_tx, replicas, local_request_id).await,
        }
    }

    fn box_clone(&self) -> Box<dyn Handler<ClientRequest, ClientResponse>> {
        Box::new(Self::new(
            self.connection_registry.box_clone(),
            self.hlc.clone(),
        ))
    }
}

pub(crate) struct ClientServer {
    server: Server<ClientRequest, ClientResponse>,
}

impl ClientServer {
    pub(crate) fn new(
        node_id: Uuid,
        core_id: Uuid,
        local_ip: Ipv4Addr,
        connection_registry: ConnectionRegistry<ServerRequest, ServerResponse>,
    ) -> Result<Self, ServerError> {
        let hlc = Arc::new(Mutex::new(uhlc::HLC::default()));
        let handler = ClientHandler::new(connection_registry, hlc);
        let server = Server::new(handler)?;

        println!("Starting Client Server on Port: {}", server.port);

        let _mdns = ClientMDNS::new(node_id, core_id, local_ip, server.port);

        Ok(Self { server })
    }

    pub(crate) async fn run(&mut self) -> Result<(), ServerError> {
        self.server.run().await
    }
}
