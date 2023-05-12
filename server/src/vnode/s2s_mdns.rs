use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use mdns_sd::{Receiver, ServiceDaemon, ServiceEvent, ServiceInfo};
use net::quic::QuicClient;
use tokio::{task, time};
use uuid::Uuid;

use crate::{
    node_registry::{ClientRegistry, Locality},
    protocol::{ServerRequest, ServerResponse},
    ServerError, VNodeId,
};

#[derive(Clone)]
pub struct S2SMDNS {
    receiver: Receiver<ServiceEvent>,
    local_socket_addr: SocketAddr,
    client_registry: ClientRegistry<ServerRequest, ServerResponse>,
    node_id: Uuid,
}

impl S2SMDNS {
    pub(crate) fn new(
        node_id: Uuid,
        core_id: Uuid,
        client_registry: ClientRegistry<ServerRequest, ServerResponse>,
        local_ip: Ipv4Addr,
        port: u16,
    ) -> Self {
        let local_socket_addr: SocketAddr = format!("{}:{}", local_ip, port).parse().unwrap();
        let mdns = ServiceDaemon::new().expect("Failed to create daemon");
        let service_type = "_quic-db-priv._udp.local.";
        let receiver = mdns.browse(service_type).expect("Failed to browse");
        let instance_name = format!("node-{}", core_id);
        let host_name = format!("{}.local.", local_ip);
        let properties = [
            ("node_id".to_string(), node_id.to_string()),
            ("core_id".to_string(), core_id.to_string()),
        ];
        let my_service = ServiceInfo::new(
            service_type,
            &instance_name,
            &host_name,
            local_ip,
            port,
            &properties[..],
        )
        .unwrap();

        mdns.register(my_service.clone())
            .expect("Failed to register our service");

        Self {
            receiver,
            local_socket_addr,
            client_registry,
            node_id,
        }
    }

    pub(crate) async fn spawn(&self) -> task::JoinHandle<Result<(), ServerError>> {
        let mut interval = time::interval(Duration::from_secs(30));
        let receiver = self.receiver.clone();
        let local_socket_addr = self.local_socket_addr.clone();
        let mut client_registry = self.client_registry.clone();
        let this_node_id = self.node_id.clone();

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
                                    let client = QuicClient::new(socket_addr)
                                        .map_err(|e| ServerError::Initialization(e.to_string()))?;

                                    client_registry
                                        .add(vnode_id, &Locality::Remote, client)
                                        .await
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
