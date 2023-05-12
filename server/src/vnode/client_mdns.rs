use std::net::Ipv4Addr;

use mdns_sd::{ServiceDaemon, ServiceInfo};
use uuid::Uuid;

#[derive(Clone)]
pub struct ClientMDNS {}

impl ClientMDNS {
    pub(crate) fn new(node_id: Uuid, core_id: Uuid, local_ip: Ipv4Addr, port: u16) -> Self {
        let mdns = ServiceDaemon::new().expect("Failed to create daemon");
        let service_type = "_quic-db-pub._udp.local.";
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

        Self {}
    }
}
