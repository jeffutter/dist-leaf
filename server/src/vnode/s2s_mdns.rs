use std::{net::SocketAddr, time::Duration};

use mdns_sd::{Receiver, ServiceDaemon, ServiceEvent, ServiceInfo};
use net::{quic::QuicClient, tcp::TcpClient};
use tokio::{task, time};
use uuid::Uuid;

use crate::{
    node_registry::Locality,
    protocol::{ServerRequest, ServerResponse},
    server_context::{ServerContext, Transport},
    ServerError, VNodeId,
};

#[derive(Clone)]
pub struct S2SMDNS {
    receiver: Receiver<ServiceEvent>,
    local_socket_addr: SocketAddr,
    context: ServerContext<ServerRequest, ServerResponse>,
}

impl S2SMDNS {
    pub(crate) fn new(context: ServerContext<ServerRequest, ServerResponse>, port: u16) -> Self {
        let local_socket_addr: SocketAddr =
            format!("{}:{}", context.local_ip, port).parse().unwrap();
        let mdns = ServiceDaemon::new().expect("Failed to create daemon");
        let service_type = "_quic-db-priv._udp.local.";
        let receiver = mdns.browse(service_type).expect("Failed to browse");
        let instance_name = format!("node-{}", context.core_id);
        let host_name = format!("{}.local.", context.local_ip);
        let properties = [
            ("node_id".to_string(), context.node_id.to_string()),
            ("core_id".to_string(), context.core_id.to_string()),
        ];
        let my_service = ServiceInfo::new(
            service_type,
            &instance_name,
            &host_name,
            context.local_ip,
            port,
            &properties[..],
        )
        .unwrap();

        mdns.register(my_service.clone())
            .expect("Failed to register our service");

        Self {
            receiver,
            local_socket_addr,
            context,
        }
    }

    pub(crate) async fn spawn(&self) -> task::JoinHandle<Result<(), ServerError>> {
        let mut interval = time::interval(Duration::from_secs(30));
        let receiver = self.receiver.clone();
        let local_socket_addr = self.local_socket_addr.clone();
        let mut client_registry = self.context.client_registry.clone();
        let this_node_id = self.context.node_id.clone();
        let transport = self.context.transport;

        task::spawn(async move {
            loop {
                interval.tick().await;
                while let Ok(event) = receiver.recv_async().await {
                    match event {
                        ServiceEvent::ServiceResolved(info) => {
                            let node_id: Uuid = info
                                .get_property_val_str("node_id")
                                .unwrap()
                                .parse()
                                .unwrap();
                            let core_id: Uuid = info
                                .get_property_val_str("core_id")
                                .unwrap()
                                .parse()
                                .unwrap();
                            if node_id != this_node_id {
                                let vnode_id = VNodeId::new(node_id, core_id);

                                log::debug!(
                                    "Resolved a new service: {} {}",
                                    info.get_fullname(),
                                    info.get_port()
                                );

                                let socket_addr: SocketAddr = SocketAddr::new(
                                    info.get_addresses()
                                        .iter()
                                        .next()
                                        .unwrap()
                                        .to_owned()
                                        .into(),
                                    info.get_port(),
                                );

                                if socket_addr != local_socket_addr {
                                    match transport {
                                        Transport::TCP => {
                                            let client =
                                                TcpClient::new(socket_addr).map_err(|e| {
                                                    ServerError::Initialization(e.to_string())
                                                })?;
                                            client_registry
                                                .add(vnode_id, &Locality::Remote, client)
                                                .await
                                        }
                                        Transport::Quic => {
                                            let client =
                                                QuicClient::new(socket_addr).map_err(|e| {
                                                    ServerError::Initialization(e.to_string())
                                                })?;
                                            client_registry
                                                .add(vnode_id, &Locality::Remote, client)
                                                .await
                                        }
                                    }
                                }
                            }
                        }
                        ServiceEvent::SearchStarted(_) => (),
                        other_event => {
                            log::debug!("Received other event: {:?}", &other_event);
                        }
                    }
                }
            }
        })
    }
}
