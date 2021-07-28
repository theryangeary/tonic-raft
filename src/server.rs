use std::net::SocketAddr;
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
    consensus_server: ConsensusServer<ConsensusModule<L>>,
    consensus_module: ConsensusModule<L>,
}

impl<L> RaftService<L>
where
    L: Log<super::consensus::Entry, String>,
{
    pub fn new(id: u64, broker_list: Vec<SocketAddr>) -> Self {
        let consensus_module = ConsensusModule::new(id, broker_list);
        let consensus_server = ConsensusServer::new(consensus_module.clone());

        Self {
            consensus_server,
            consensus_module,
        }
    }

    pub fn router(&self) -> Router<ConsensusServer<ConsensusModule<L>>, Unimplemented> {
        Server::builder().add_service(self.consensus_server.clone())
    }

    pub async fn append_transition<T>(&self, transition: &T) -> Result<(), String>
    where
        T: Transition,
    {
        self.consensus_module.append_transition(transition).await
    }
}
