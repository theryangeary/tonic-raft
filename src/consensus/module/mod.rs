use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::RwLock;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;

use crate::{Log, Role, Entry};
use crate::log::Transition;

mod new;
mod consensus;

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
    /// channel to watch to get a sender for submitting log entries
    log_entry_tx_watch_rx: watch::Receiver<mpsc::Sender<Entry>>,
    log_entry_tx_watch_tx: Arc<watch::Sender<mpsc::Sender<Entry>>>,
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
async fn set_role(&self, new_role: Role) {
    let mut r = self.role.write().await;
    *r = new_role;
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
/// This should be used over manually calling `AtomicU64::store`
fn set_current_term(&self, new_term: u64) {
    self.current_term.store(new_term, Ordering::SeqCst);
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

/// Check if the term from an incoming RPC is higher than the previous highest known term, and
/// update accordingly
///
/// Intended to be called by all RPC receiving functions, before handling specific logic.
async fn term_check(&self, rpc_term: u64) {
    let current_term = self.current_term();
    if rpc_term > current_term {
        self.set_current_term(rpc_term);
        self.set_role(Role::Follower).await;
    }
}
}
