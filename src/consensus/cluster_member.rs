use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Debug, Clone)]
pub struct ClusterMember {
    /// Address for connecting to this cluster member
    ///
    /// N.B. That this is effectively read-only, i.e. there is no changing the socket for a member.
    /// If a member actually changes addresses, make a new instance of this struct.
    socket_address: Arc<SocketAddr>,
    /// Index of the next log entry to send to this server (initialized to leader's last log index
    /// + 1)
    next_index: Arc<AtomicU64>,
    /// Index of highest log entry known to be replicated on server
    match_index: Arc<AtomicU64>,
}

impl ClusterMember {
    pub fn new(socket_address: SocketAddr) -> Self {
        ClusterMember {
            socket_address: Arc::new(socket_address),
            next_index: Arc::new(AtomicU64::new(0)),
            match_index: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn next_index(&self) -> u64 {
        self.next_index.load(Ordering::SeqCst)
    }

    pub fn match_index(&self) -> u64 {
        self.match_index.load(Ordering::SeqCst)
    }

    pub fn socket_address(&self) -> SocketAddr {
        *self.socket_address
    }
}

impl std::fmt::Display for ClusterMember {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClusterMember at {} with next_index={}, match_index={}",
            self.socket_address(),
            self.next_index(),
            self.match_index()
        )
    }
}
