use std::net::SocketAddr;
use tonic::transport::server::{Router, Unimplemented};
use tonic::transport::Server;

use super::consensus::{ConsensusModule, ConsensusServer, Entry};
use super::entry_appender::EntryAppender;
use super::log::Log;

pub struct RaftServer<L>
where
    L: Log<Entry, String> + 'static,
{
    consensus_service: ConsensusServer<ConsensusModule<L>>,
    entry_appender: EntryAppender,
}

impl<L> RaftServer<L>
where
    L: Log<super::consensus::Entry, String>,
{
    pub fn new(id: u64, broker_list: Vec<SocketAddr>) -> Self {
        let consensus_module = ConsensusModule::new(id, broker_list);
        let entry_appender = consensus_module.entry_appender();
        let consensus_service = ConsensusServer::new(consensus_module);

        Self {
            consensus_service,
            entry_appender,
        }
    }

    pub fn router(&self) -> Router<ConsensusServer<ConsensusModule<L>>, Unimplemented> {
        Server::builder().add_service(self.consensus_service.clone())
    }

    pub fn entry_appender(&self) -> EntryAppender {
        self.entry_appender.clone()
    }
}
