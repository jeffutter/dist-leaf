use crate::{
    message_clients::MessageClients,
    protocol::{ServerRequest, ServerResponse},
    vnode::client_mdns::ClientMDNS,
    ServerError,
};
use bytes::Bytes;
use futures::StreamExt;
use itertools::Itertools;
use quic_client::protocol::{ClientRequest, ClientResponse};
use quic_transport::{DataStream, Encode, MessageStream};
use s2n_quic::{connection, stream::BidirectionalStream, Server};
use std::{net::Ipv4Addr, sync::Arc, usize};
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
        let mut join_set: JoinSet<Result<ClientResponse, ServerError>> = JoinSet::new();
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
            let client_request_id = client_request_id.clone();
            let server_request = server_request.clone();

            join_set.spawn({
                async move {
                    let res = connection.box_clone().request(server_request).await?;
                    let res: ClientResponse = match res {
                        ServerResponse::Error { error, .. } => ClientResponse::Error {
                            request_id: client_request_id,
                            error,
                        },
                        ServerResponse::Result { result, .. } => ClientResponse::Result {
                            request_id: client_request_id,
                            result,
                        },
                        ServerResponse::Ok { .. } => ClientResponse::Ok(client_request_id),
                    };

                    Ok::<ClientResponse, ServerError>(res)
                }
            });
        }

        let mut results: Vec<Option<Result<ClientResponse, ServerError>>> = Vec::new();

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(res)) => results.push(Some(Ok(res))),
                Ok(Err(e)) => results.push(Some(Err(e))),
                Err(_e) => results.push(Some(Err(ServerError::Unknown))),
            }

            if results.len() >= CONSISTENCY_LEVEL {
                let unique_res: Vec<ClientResponse> = results
                    .iter()
                    .filter_map(|x| match x {
                        Some(Ok(res)) => Some(res.clone()),
                        _ => None,
                    })
                    .unique()
                    .collect();

                if unique_res.len() == 1 {
                    let res = &unique_res[0];

                    send_tx
                        .send(res.encode())
                        .await
                        .expect("channel should be open");

                    event!(
                        Level::INFO,
                        "Results matched with {} request(s)",
                        results.len()
                    );

                    break;
                } else if results.len() == REPLICATION_FACTOR {
                    let res = ServerResponse::Error {
                        request_id: client_request_id,
                        error: "Results did not match".to_string(),
                    };
                    send_tx
                        .send(res.encode())
                        .await
                        .expect("channel should be open");

                    event!(Level::ERROR, results = ?results, "Results did not match");
                }
            }
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
