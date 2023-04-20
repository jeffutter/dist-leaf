use crate::{
    message_clients::MessageClients,
    protocol::{ServerRequest, ServerResponse},
    vnode::client_mdns::ClientMDNS,
    ServerError,
};
use bytes::Bytes;
use futures::StreamExt;
use quic_client::protocol::{ClientRequest, ClientResponse};
use quic_transport::{DataStream, Encode, MessageStream};
use s2n_quic::{connection, stream::BidirectionalStream, Server};
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc, usize};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinSet,
};
use tracing::{event, instrument, Level};
use uuid::Uuid;

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));
/// NOTE: this certificate is to be used for demonstration purposes only!
pub static KEY_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/key.pem"));

static REPLICATION_FACTOR: usize = 3;
static CONSISTENCY_LEVEL: usize = match (REPLICATION_FACTOR / 2, REPLICATION_FACTOR % 2) {
    (n, 0) => n,
    (n, _) => n + 1,
};

pub(crate) struct ClientServer {
    clients: Arc<Mutex<MessageClients>>,
    hlc: Arc<Mutex<uhlc::HLC>>,
    storage: db::Database,
    server: s2n_quic::Server,
}

impl ClientServer {
    pub(crate) fn new(
        node_id: Uuid,
        core_id: Uuid,
        local_ip: Ipv4Addr,
        storage: db::Database,
        clients: Arc<Mutex<MessageClients>>,
    ) -> Result<Self, ServerError> {
        let server = Server::builder()
            .with_tls((CERT_PEM, KEY_PEM))
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .with_io("0.0.0.0:0")
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .start()
            .map_err(|e| ServerError::Initialization(e.to_string()))?;

        let port = server
            .local_addr()
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .port();

        println!("Starting Client Server on Port: {}", port);

        let _mdns = ClientMDNS::new(node_id, core_id, local_ip, port);

        Ok(Self {
            clients,
            storage,
            server,
            hlc: Arc::new(Mutex::new(uhlc::HLC::default())),
        })
    }

    #[instrument(skip(clients, send_tx, hlc))]
    async fn handle_request(
        req: ClientRequest,
        clients: Arc<Mutex<MessageClients>>,
        storage: db::Database,
        hlc: Arc<Mutex<uhlc::HLC>>,
        send_tx: mpsc::Sender<Bytes>,
    ) -> Result<(), ServerError> {
        let mut join_set: JoinSet<Result<ServerResponse, ServerError>> = JoinSet::new();
        let mut cs = clients.lock().await;
        let replicas = cs.replicas(req.key(), REPLICATION_FACTOR).await;

        event!(Level::DEBUG, "replicas" = ?replicas);

        let local_request_id = hlc.lock().await.new_timestamp();
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

        for connection in replicas {
            let server_request = server_request.clone();

            join_set.spawn({
                async move {
                    let res = connection.box_clone().request(server_request).await?;

                    Ok::<_, ServerError>(res)
                }
            });
        }

        let mut results: HashMap<ServerResponse, usize> = HashMap::new();
        let mut received_responses = 0;
        let mut result_sent = false;

        while let Some(res) = join_set.join_next().await {
            received_responses += 1;

            let r = match res {
                Ok(Ok(ServerResponse::Error { error, .. })) => Err(ServerError::Response(error)),
                Ok(Ok(res)) => Ok(res),
                Ok(Err(e)) => Err(e),
                Err(_e) => Err(ServerError::Unknown),
            };

            match r {
                Ok(r) => {
                    results
                        .entry(r.clone())
                        .and_modify(|i| *i += 1)
                        .or_insert(1);

                    if results[&r] >= CONSISTENCY_LEVEL {
                        let response = match r {
                            ServerResponse::Ok { .. } => ClientResponse::Ok(client_request_id),
                            ServerResponse::Error { .. } => unreachable!(),
                            ServerResponse::Result { result, .. } => ClientResponse::Result {
                                result,
                                request_id: client_request_id,
                            },
                        };

                        send_tx
                            .send(response.encode())
                            .await
                            .expect("channel sould be open");

                        result_sent = true;

                        event!(
                            Level::INFO,
                            "Results matched with {} request(s)",
                            results.len()
                        );

                        break;
                    }

                    if received_responses >= REPLICATION_FACTOR {
                        let res = ClientResponse::Error {
                            request_id: client_request_id,
                            error: "Results did not match".to_string(),
                        };

                        send_tx
                            .send(res.encode())
                            .await
                            .expect("channel should be open");
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

            send_tx
                .send(res.encode())
                .await
                .expect("channel should be open");
        }

        // For put requests, we want to let all of the writes finish, even if we've hit the
        // requested Consistency Level
        // if let ClientRequest::Put { .. } = req {
        if is_put {
            join_set.detach_all();
        }

        Ok::<(), ServerError>(())
    }

    async fn handle_stream(
        stream: BidirectionalStream,
        clients: Arc<Mutex<MessageClients>>,
        storage: db::Database,
        hlc: Arc<Mutex<uhlc::HLC>>,
    ) {
        let (receive_stream, mut send_stream) = stream.split();
        let data_stream = DataStream::new(receive_stream);
        let mut request_stream: MessageStream<ClientRequest> = MessageStream::new(data_stream);

        let (send_tx, mut send_rx): (mpsc::Sender<Bytes>, mpsc::Receiver<Bytes>) =
            mpsc::channel(100);

        tokio::spawn(async move {
            while let Some(data) = send_rx.recv().await {
                send_stream.send(data).await?;
            }

            Ok::<(), ServerError>(())
        });

        while let Some(Ok((req, _data))) = request_stream.next().await {
            let storage = storage.clone();
            let clients = clients.clone();
            let send_tx = send_tx.clone();
            let hlc = hlc.clone();
            tokio::spawn(
                async move { Self::handle_request(req, clients, storage, hlc, send_tx).await },
            );
        }
    }

    async fn handle_connection(
        mut connection: connection::Connection,
        clients: Arc<Mutex<MessageClients>>,
        storage: db::Database,
        hlc: Arc<Mutex<uhlc::HLC>>,
    ) {
        while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
            tokio::spawn(Self::handle_stream(
                stream,
                clients.clone(),
                storage.clone(),
                hlc.clone(),
            ));
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), ServerError> {
        let storage = self.storage.clone();
        let hlc = self.hlc.clone();

        while let Some(connection) = self.server.accept().await {
            tokio::spawn(Self::handle_connection(
                connection,
                self.clients.clone(),
                storage.clone(),
                hlc.clone(),
            ));
        }

        Ok(())
    }
}
