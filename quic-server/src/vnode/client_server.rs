use crate::{
    node_registry::ConnectionRegistry,
    protocol::{ServerRequest, ServerResponse},
    vnode::{client_mdns::ClientMDNS, VNodeId},
};
use async_trait::async_trait;
use futures::{channel::mpsc, SinkExt};
use net::quic::{Handler, Server, ServerError};
use quic_client::protocol::{ClientRequest, ClientResponse};
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc, usize};
use tokio::{sync::Mutex, task::JoinSet};
use tracing::{event, instrument, Level};
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

#[async_trait]
impl Handler<ClientRequest, ClientResponse> for ClientHandler {
    #[instrument(skip(self, send_tx))]
    async fn call(
        &mut self,
        req: ClientRequest,
        mut send_tx: mpsc::Sender<ClientResponse>,
    ) -> Result<(), ServerError> {
        let mut join_set: JoinSet<Result<(VNodeId, ServerResponse), ServerError>> = JoinSet::new();
        let req_key = req.key().clone();
        let replicas = self
            .connection_registry
            .replicas(&req_key, REPLICATION_FACTOR)
            .await;

        event!(Level::INFO, "Fetching {:?} from: {:?}", req, replicas);

        let local_request_id = self.hlc.lock().await.new_timestamp();
        let client_request_id = req.request_id().clone();
        let is_put = match req {
            ClientRequest::Put { .. } => true,
            _ => false,
        };

        let server_request = match req {
            ClientRequest::Get { key, .. } => ServerRequest::Get {
                request_id: local_request_id,
                key,
            },
            ClientRequest::Put { key, value, .. } => ServerRequest::Put {
                request_id: local_request_id,
                key,
                value,
            },
        };

        for (vnode_id, connection) in replicas.iter() {
            let vnode_id = vnode_id.clone();
            let mut connection = connection.box_clone();

            let server_request = server_request.clone();

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
                Ok(Ok((_, ServerResponse::Error { error, .. }))) => {
                    Err(ServerError::Response(error))
                }
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
                            ServerResponse::Result { result, .. } => ClientResponse::Result {
                                result: result.clone(),
                                request_id: client_request_id,
                            },
                        };

                        send_tx.send(response).await.expect("channel sould be open");

                        result_sent = true;

                        event!(
                            Level::INFO,
                            "Results matched with {} request(s)",
                            results[&r].0
                        );

                        if let ServerResponse::Result {
                            request_id: _,
                            data_id: Some(data_id),
                            result: Some(data),
                        } = &r
                        {
                            results.remove(&r);
                            let repair_request = ServerRequest::Put {
                                request_id: *data_id,
                                key: req_key,
                                value: data.to_string(),
                            };

                            let bad_nodes: Vec<VNodeId> = results
                                .into_iter()
                                .flat_map(|(_, (_, vnode_id))| vnode_id)
                                .collect();

                            for (vnode_id, mut connection) in replicas {
                                if bad_nodes.contains(&vnode_id) {
                                    connection.request(repair_request.clone()).await?;
                                }
                            }
                        }

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

        // For put requests, we want to let all of the writes finish, even if we've hit the
        // requested Consistency Level
        // if let ClientRequest::Put { .. } = req {
        if is_put {
            join_set.detach_all();
        }

        Ok::<(), ServerError>(())
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
