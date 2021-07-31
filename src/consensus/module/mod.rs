use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::RwLock;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

use crate::log::Transition;
use crate::{Entry, Log, Role};

mod consensus;
mod new;

#[derive(Debug, Clone)]
/// Raft consensus module
///
/// Struct members are broken into sections based on the raft paper, Figure 2, with an `extra`
/// section for implementation details not addressed in the paper: https://raft.github.io/raft.pdf
pub struct ConsensusModule<L>
where
    L: Log<Entry, String>,
{
    // persistent state: TODO persist these data to disk before responding to RPCs
    /// latest term server has seen (initialized to 0 on first boot, increases monotonically)
    current_term: Arc<AtomicU64>,
    /// candidate_id that received vote in current term (or None)
    voted_for: Arc<RwLock<Option<u64>>>,
    /// log entries; each entry contains command for state machine, and term when entry was
    /// received by leader (first index is 1)
    log: L,
    // volatile state
    /// index of highest log entry known to be committed (initialized to 0, increases
    /// monotonically)
    commit_index: Arc<AtomicU64>,
    /// index of highest log entry applied to state machine (initialized to 0, increases
    /// monotonically)
    last_applied: Arc<AtomicU64>,
    // volatile state if leader, otherwise None. Reinitialized after election.
    /// for each server, index of the next log entry to send to that server (initialized to leader
    /// last log index + 1)
    next_index: Arc<RwLock<HashMap<SocketAddr, u64>>>,
    /// for each server, index of highest log entry known to be replicated on server (initialized
    /// to 0, increases monotonically)
    match_index: Arc<RwLock<HashMap<SocketAddr, u64>>>,
    // extra
    /// id of this raft broker
    id: Arc<AtomicU64>,
    /// role this server is currently acting in
    role: Arc<RwLock<Role>>,
    /// list of brokers in cluster
    brokers: Arc<RwLock<Vec<SocketAddr>>>,
    /// handles for tasks
    task_handles: Arc<RwLock<Vec<JoinHandle<()>>>>,
    /// channel to reset election timeout
    election_timeout_reset_tx: broadcast::Sender<()>,
    /// channel to queue role transitions
    role_transition_tx: mpsc::Sender<Role>,
}

impl<L> ConsensusModule<L>
where
    L: Log<Entry, String> + 'static,
{
    /// Add a replicated state machine transition to the event log
    pub async fn append_transition<T>(&self, transition: &T) -> Result<u64, String>
    where
        T: Transition,
    {
        {
            // only append if leader - otherwise the client should be redirected to the leader
            // (TODO)
            let r = self.role.read().await;
            if *r != Role::Leader {
                return Err(String::from("no, I am not leader"));
            }
        }

        println!("Appending transition: {:?}", transition);
        let log_index = self
            .log
            .append(Entry {
                term: self.current_term.load(Ordering::SeqCst),
                data: bincode::serialize(transition).map_err(|e| e.to_string())?,
            })
            .await?;
        // TODO only return once transition is replicated to a majority
        // TODO call set_commit_index(log_index) after replicating to majority
        Ok(log_index)
    }

    /// Set current role being performed by the server
    ///
    /// N.B. that this only stores the value of the role, it does not begin the processes that should
    /// run when acting as that role. See `role_transition_tx` and `become_role` for that.
    async fn set_role(&self, new_role: Role) {
        let mut r = self.role.write().await;
        *r = new_role;
    }

    /// Become role and start behavior accordingly
    async fn become_role(&self, new_role: Role) -> Result<(), mpsc::error::SendError<Role>> {
        self.role_transition_tx.send(new_role).await
    }

    /// Get current term
    ///
    /// This function loads an atomic, so callers should store the result as a local variable
    /// to reduce contention unless callers require the atomicity.
    fn current_term(&self) -> u64 {
        self.current_term.load(Ordering::SeqCst)
    }

    /// Set current term
    ///
    /// This should be used over manually calling `AtomicU64::store`. It should also only be used in
    /// cases where the current term must specifically be set to a given value. In the happy case of
    /// incrementing the current term by one, use `increment_current_term`.
    async fn set_current_term(&self, new_term: u64) {
        self.current_term.store(new_term, Ordering::SeqCst);
    }

    /// Increment current term
    ///
    /// This should typically be used when incrementing `current_term`, instead of using
    /// `set_current_term`, because incrementing usually happens in the happy/error free case, in which
    /// case votes must be reset
    async fn increment_current_term(&self) {
        let _previous_voted_for = self.voted_for.write().await.take();
        self.current_term.fetch_add(1, Ordering::SeqCst);
    }

    /// Get current commit_index
    ///
    /// This function loads an atomic, so callers should store the result as a local variable to
    /// reduce contention unless callers require the atomicity.
    fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::SeqCst)
    }

    /// Set current commit_index
    ///
    /// This should be used over manually calling `AtomicU64::store`
    fn set_commit_index(&self, new_commit_index: u64) {
        self.commit_index.store(new_commit_index, Ordering::SeqCst);
    }

    /// Get current last_applied
    ///
    /// This function loads an atomic, so callers should store the result as a local variable to
    /// reduce contention unless callers require the atomicity.
    fn last_applied(&self) -> u64 {
        self.last_applied.load(Ordering::SeqCst)
    }

    /// Set current last_applied
    ///
    /// This should be used over manually calling `AtomicU64::store`
    pub fn set_last_applied(&self, new_last_applied: u64) {
        self.last_applied.store(new_last_applied, Ordering::SeqCst);
    }

    /// Get server id
    pub fn id(&self) -> u64 {
        self.id.load(Ordering::SeqCst)
    }

    /// Check if the term from an incoming RPC is higher than the previous highest known term, and
    /// update accordingly
    ///
    /// Intended to be called by all RPC receiving functions, before handling specific logic.
    async fn term_check(&self, rpc_term: u64) {
        if rpc_term > self.current_term() {
            self.set_current_term(rpc_term).await;
            println!("received RPC with term > self.current_term, reverting to follower");
            self.set_role(Role::Follower).await;
        }
    }
}
