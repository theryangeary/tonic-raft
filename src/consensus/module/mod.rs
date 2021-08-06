use futures::stream::{Stream, StreamExt};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::RwLock;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use crate::consensus::ClusterMember;
use crate::log::Transition;
use crate::raft::{consensus_client::ConsensusClient, AppendEntriesRequest};
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
    // extra
    /// id of this raft broker
    id: Arc<AtomicU64>,
    /// role this server is currently acting in
    role: Arc<RwLock<Role>>,
    /// list of brokers in cluster
    cluster_members: Arc<RwLock<Vec<ClusterMember>>>,
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
    ///
    /// Returns Ok(n) if the transition is added to the log and the log entry is replicated to a
    /// majority of cluster members. n is the index of the new log entry.
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
        let sc = self.clone();
        let mut append_entries_results_stream = self
            .for_each_cluster_member(|cluster_member| async move {
                // append entries
                let mut client =
                    ConsensusClient::connect(format!("http://{}", cluster_member.socket_address()))
                        .await
                        .map_err(|e| format!("failed to connect: {:?}", e))?;

                let request = {
                    // TODO set values properly
                    Request::new(AppendEntriesRequest {
                        term: sc.current_term(),
                        leader_id: sc.id(),
                        prev_log_index: sc.log.last_index().await,
                        prev_log_term: sc.log.last_term().await,
                        entries: vec![],
                        leader_commit: sc.commit_index(),
                    })
                };

                // TODO retry until the follower is up to date
                return match client.append_entries(request).await {
                    Ok(_) => Ok(()),
                    Err(response_error) => Err(response_error.to_string()),
                };
            })
            .await
            .filter_map(|result| match result {
                Err(e) => {
                    eprintln!("failed to append entries: {:?}", e);
                    None
                }
                Ok(t) => Some(t),
            });

        while let Some(append_entry_result) = append_entries_results_stream.next().await {
            // TODO do something
        }

        //TODO if a majority were successful, udpate commit index

        // TODO call set_commit_index(log_index) after replicating to majority
        Ok(log_index)
    }

    /// Get current role
    async fn role(&self) -> Role {
        *self.role.read().await
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
    /// This should be used over manually calling `AtomicU64::store`.
    async fn set_current_term(&self, new_term: u64) {
        let _previous_voted_for = self.voted_for.write().await.take();
        self.current_term.store(new_term, Ordering::SeqCst);
    }

    /// Increment current term
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

    async fn reset_election_timeout(&self) -> Result<(), broadcast::error::SendError<()>> {
        if self.role().await == Role::Follower
            && self.election_timeout_reset_tx.receiver_count() > 0
        {
            self.election_timeout_reset_tx.send(())?;
        }

        Ok(())
    }

    /// Check if the term from an incoming RPC is higher than the previous highest known term, and
    /// update accordingly
    ///
    /// Intended to be called by all RPC receiving functions, before handling specific logic.
    async fn term_check(&self, rpc_term: u64) {
        if rpc_term > self.current_term() {
            self.set_current_term(rpc_term).await;
            if self.role().await != Role::Follower {
                println!("received RPC with term > self.current_term, reverting to follower");
                if let Err(e) = self.become_role(Role::Follower).await {
                    println!("Failed to become Follower: {:?}", e);
                }
            }
        }
    }

    /// Get number of brokers meant to be in cluster
    async fn num_brokers(&self) -> usize {
        self.cluster_members.read().await.len()
    }

    /// Get number of brokers needed as a minimum to constitute a majority
    async fn majority(&self) -> usize {
        self.cluster_members.read().await.len() / 2 + 1
    }

    /// Get cluster_members
    async fn cluster_members(&self) -> Vec<ClusterMember> {
        self.cluster_members.read().await.to_vec()
    }

    /// Get Iterator over socket addresses of all cluster members
    async fn cluster_member_socket_addresses(&self) -> Vec<SocketAddr> {
        self.cluster_members
            .read()
            .await
            .iter()
            .map(ClusterMember::socket_address)
            .collect()
    }

    /// Perform the same operation for each broker, in parallel (in new tasks), and return the
    /// results as a stream, on a first-complete-first-returned basis
    async fn for_each_cluster_member<F, Fut, T, E>(&self, op: F) -> impl Stream<Item = Result<T, E>>
    where
        E: std::fmt::Debug + Send + 'static,
        T: std::fmt::Debug + Send + 'static,
        F: FnOnce(ClusterMember) -> Fut + Send + 'static + Clone,
        Fut: Future<Output = Result<T, E>> + Send,
    {
        let cluster_member_list = self.cluster_members().await;
        let mut handle_list = Vec::new();
        let (tx, rx) = mpsc::channel(cluster_member_list.len());

        for b in &*cluster_member_list {
            let cluster_member = b.clone();
            let t = tx.clone();
            let opc = op.clone();
            let handle = tokio::spawn(async move {
                let result = opc(cluster_member).await;
                if let Err(e) = t.send(result).await {
                    eprintln!("Failed to send result to stream, receiver hung up: {:?}", e);
                }
            });
            handle_list.push(handle);
        }

        return ReceiverStream::new(rx);
    }
}
