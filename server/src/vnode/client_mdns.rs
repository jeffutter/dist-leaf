use mdns_sd::{ServiceDaemon, ServiceInfo};

use crate::{
    protocol::{ServerRequest, ServerResponse},
    server_context::ServerContext,
};

#[derive(Clone)]
pub struct ClientMDNS {}

impl ClientMDNS {
    pub(crate) fn new(context: ServerContext<ServerRequest, ServerResponse>, port: u16) -> Self {
        let mdns = ServiceDaemon::new().expect("Failed to create daemon");
        let service_type = "_quic-db-pub._udp.local.";
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

        Self {}
    }
}
