use std::net::SocketAddr;
use tonic::transport::server::{Router, Unimplemented};
use tonic::transport::Server;

use super::consensus::{ConsensusModule, ConsensusServer, Entry};
use super::log::Log;

pub struct RaftServer<L>
where
    L: Log<Entry, String> + 'static,
{
    consensus_service: ConsensusServer<ConsensusModule<L>>,
}

impl<L> RaftServer<L>
where
    L: Log<super::consensus::Entry, String>,
{
    pub fn new(id: u64, broker_list: Vec<SocketAddr>) -> Self {
        let consensus_module = ConsensusModule::new(id, broker_list);
        let consensus_service = ConsensusServer::new(consensus_module);

        Self { consensus_service }
    }

    pub fn router(&self) -> Router<ConsensusServer<ConsensusModule<L>>, Unimplemented> {
        Server::builder().add_service(self.consensus_service.clone())
    }
}