use std::net::SocketAddr;
use std::ops::Deref;
use tonic::transport::server::{Router, Unimplemented};
use tonic::transport::Server;

use super::consensus::{ConsensusModule, ConsensusServer, Entry};
use super::entry_appender::EntryAppender;
use super::log::{Log, Transition};

#[derive(Debug, Clone)]
pub struct RaftService<L>
where
    L: Log<Entry, String> + 'static,
{
    consensus_module: ConsensusModule<L>,
}

impl<L> RaftService<L>
where
    L: Log<super::consensus::Entry, String>,
{
    pub fn new(id: u64, broker_list: Vec<SocketAddr>) -> Self {
        let consensus_module = ConsensusModule::new(id, broker_list);

        Self { consensus_module }
    }

    pub fn router(&self) -> Router<ConsensusServer<ConsensusModule<L>>, Unimplemented> {
        Server::builder().add_service(ConsensusServer::new(self.consensus_module.clone()))
    }
}

impl<L> Deref for RaftService<L>
where
    L: Log<super::consensus::Entry, String>,
{
    type Target = ConsensusModule<L>;

    fn deref(&self) -> &Self::Target {
        &self.consensus_module
    }
}
