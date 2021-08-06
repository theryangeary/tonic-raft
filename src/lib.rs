pub mod consensus;
pub mod log;
pub mod server;

pub mod raft {
    tonic::include_proto!("raft");
}

pub use consensus::{ConsensusModule, Role};
pub use log::Log;
pub use raft::{consensus_server::ConsensusServer, Entry};
