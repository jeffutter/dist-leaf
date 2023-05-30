use std::{net::Ipv4Addr, path::PathBuf};

use clap::ValueEnum;
use uuid::Uuid;

use crate::{node_registry::ClientRegistry, vnode::VNodeId};

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum Transport {
    TCP,
    Quic,
}

#[derive(Clone, Debug)]
pub(crate) struct ServerContext<Req, Res>
where
    Req: Clone,
    Res: Clone,
{
    pub(crate) transport: Transport,
    pub(crate) vnode_id: VNodeId,
    pub(crate) node_id: Uuid,
    pub(crate) core_id: Uuid,
    pub(crate) data_path: PathBuf,
    pub(crate) local_ip: Ipv4Addr,
    pub(crate) client_registry: ClientRegistry<Req, Res>,
}

impl<Req, Res> ServerContext<Req, Res>
where
    Req: Clone,
    Res: Clone,
{
    pub fn new(
        node_id: Uuid,
        core_id: Uuid,
        transport: Transport,
        local_ip: Ipv4Addr,
        data_path: PathBuf,
        client_registry: ClientRegistry<Req, Res>,
    ) -> Self {
        Self {
            vnode_id: VNodeId::new(node_id, core_id),
            transport,
            local_ip,
            data_path,
            client_registry,
            node_id,
            core_id,
        }
    }
}
