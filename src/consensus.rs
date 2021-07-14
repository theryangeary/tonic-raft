use crate::log::Log;
use raft::consensus_client::ConsensusClient;
use raft::consensus_server::Consensus;
use raft::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tonic::{Request, Response, Status};

pub use raft::consensus_server::ConsensusServer;
pub use raft::Entry;

pub mod raft {
    tonic::include_proto!("raft");
}

/// Set current role being performed by the server
async fn set_role(role: &Arc<RwLock<Role>>, new_role: Role) {
    let mut role = role.write().await;
    *role = new_role;
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

#[derive(Debug)]
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
    last_applied: u64,
    // volatile state if leader, otherwise None. Reinitialized after election.
    /// for each server, index of the next log entry to send to that server (initialized to leader
    /// last log index + 1)
    next_index: Arc<RwLock<Option<Vec<u64>>>>,
    /// for each server, index of highest log entry known to be replicated on server (initialized
    /// to 0, increases monotonically)
    match_index: Arc<RwLock<Option<Vec<u64>>>>,
    // extra
    /// id of this raft broker
    id: Arc<AtomicU64>,
    /// role this server is currently acting in
    role: Arc<RwLock<Role>>,
    /// list of brokers in cluster
    brokers: Arc<RwLock<Vec<SocketAddr>>>,
    /// handles for tasks
    task_handles: Vec<JoinHandle<()>>,
    /// channel to reset election timeout
    election_timeout_reset_tx: broadcast::Sender<()>,
    /// channel to queue role transitions
    role_transition_tx: mpsc::Sender<Role>,
}

impl<L> ConsensusModule<L>
where
    L: Log<Entry, String> + 'static,
{
    pub fn new(id: u64, broker_list: Vec<SocketAddr>) -> Self {
        let (role_transition_tx, mut role_transition_rx) = mpsc::channel(1);
        let (election_timeout_reset_tx, _) = broadcast::channel(1);
        let current_term = Arc::new(AtomicU64::new(0));
        let id = Arc::new(AtomicU64::new(id));
        let log = L::default();
        let role = Arc::new(RwLock::new(Role::Follower));
        let commit_index = Arc::new(AtomicU64::new(0));
        let brokers = Arc::new(RwLock::new(broker_list));
        let next_index = Arc::new(RwLock::new(None));
        let match_index = Arc::new(RwLock::new(None));
        // set thread to handle role transitions
        let election_timeout_reset_tx2 = election_timeout_reset_tx.clone();
        let role_transition_tx2 = role_transition_tx.clone();
        let current_term_handle = Arc::clone(&current_term);
        let id_handle = Arc::clone(&id);
        let log_handle = log.clone();
        let role_handle = Arc::clone(&role);
        let commit_index_handle = Arc::clone(&commit_index);
        let brokers_handle = Arc::clone(&brokers);
        let next_index_handle = Arc::clone(&next_index);
        let _match_index_handle = Arc::clone(&match_index);

        let _role_transition_task = tokio::spawn(async move {
            while let Some(role) = role_transition_rx.recv().await {
                let role_transition_tx = role_transition_tx2.clone();
                set_role(&role_handle, role).await;
                match role {
                    Role::Follower => {
                        // set thread to handle election timeout
                        let mut election_timeout_reset_rx = election_timeout_reset_tx2.subscribe();
                        let _election_timeout_task = tokio::spawn(async move {
                            if let Err(_elapsed) = timeout(
                                Duration::from_millis(150u64 + (rand::random::<u64>() % 150u64)),
                                election_timeout_reset_rx.recv(),
                            )
                            .await
                            {
                                let _send_result = role_transition_tx.send(Role::Candidate).await;
                            }
                        });
                    }
                    Role::Candidate => {
                        let broker_list = brokers_handle.read().await;
                        let votes_needed_to_become_leader = broker_list.len() / 2 + 1;
                        let mut votes = 0;
                        for broker in &*broker_list {
                            // request vote, increment vote count on success
                            let mut client = ConsensusClient::connect(format!("http://{}", broker))
                                .await
                                .unwrap();

                            let request = Request::new(RequestVoteRequest {
                                term: current_term_handle.load(Ordering::SeqCst),
                                candidate_id: id_handle.load(Ordering::SeqCst),
                                last_log_index: log_handle.last_index().await,
                                last_log_term: log_handle.last_term().await,
                            });

                            let response = client.request_vote(request).await.unwrap();
                            let inner = response.into_inner();
                            if inner.vote_granted {
                                votes += 1;
                            }
                        }
                        if votes >= votes_needed_to_become_leader {
                            println!(
                                "broker {} becomes the leader",
                                id_handle.load(Ordering::SeqCst)
                            );
                            let _send_result = role_transition_tx.send(Role::Leader).await;
                        }
                        println!(
                            "broker {} got {} votes",
                            id_handle.load(Ordering::SeqCst),
                            votes
                        );
                    }
                    Role::Leader => {
                        let id_handle = Arc::clone(&id_handle);
                        let commit_index_handle = Arc::clone(&commit_index_handle);
                        let current_term_handle = Arc::clone(&current_term_handle);
                        let next_index_handle = Arc::clone(&next_index_handle);
                        let log_handle = log_handle.clone();

                        let heartbeat_broker_list = Arc::clone(&brokers_handle);

                        let heartbeat_task = tokio::spawn(async move {
                            let mut interval =
                                tokio::time::interval(tokio::time::Duration::from_millis(100));

                            loop {
                                let broker_socket_addrs = heartbeat_broker_list.read().await;

                                {
                                    let mut next_index = next_index_handle.write().await;
                                    next_index.insert(vec![
                                        log_handle.last_index().await;
                                        broker_socket_addrs.len()
                                    ]);
                                }

                                let prev_log_index = log_handle.last_index().await;
                                let prev_log_term = log_handle.last_term().await;

                                for broker_socket_addr in &*broker_socket_addrs {
                                    let mut client = ConsensusClient::connect(format!(
                                        "http://{}",
                                        broker_socket_addr
                                    ))
                                    .await
                                    .unwrap();

                                    let request = {
                                        Request::new(AppendEntriesRequest {
                                            term: current_term_handle.load(Ordering::SeqCst),
                                            leader_id: id_handle.load(Ordering::SeqCst),
                                            prev_log_index,
                                            prev_log_term,
                                            entries: vec![],
                                            leader_commit: commit_index_handle
                                                .load(Ordering::SeqCst),
                                        })
                                    };

                                    let _response = client.append_entries(request).await.unwrap();
                                }

                                interval.tick().await;
                            }
                        });

                        let _heartbeat_result = heartbeat_task.await;
                    }
                }
            }
        });

        // start the server off as a follower
        let role_transition_tx2 = role_transition_tx.clone();
        tokio::spawn(async move {
            let _send_result = role_transition_tx2.send(Role::Follower).await;
        });

        Self {
            brokers,
            commit_index,
            current_term,
            election_timeout_reset_tx,
            role_transition_tx,
            last_applied: 0,
            log,
            match_index,
            next_index,
            role: Arc::new(RwLock::new(Role::Follower)),
            task_handles: vec![],
            voted_for: Arc::new(RwLock::new(None)),
            id,
        }
    }

    /// Set current role being performed by the server
    async fn set_role(&self, new_role: Role) {
        set_role(&self.role, new_role).await;
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

#[tonic::async_trait]
impl<L> Consensus for ConsensusModule<L>
where
    L: Log<Entry, String> + 'static,
{
    async fn append_entries(
        &self,
        req: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let inner = req.into_inner();

        self.term_check(inner.term).await;
        let current_term = self.current_term();

        let response = |success: bool| -> Response<AppendEntriesResponse> {
            Response::new(AppendEntriesResponse {
                term: current_term,
                success,
            })
        };

        // just say we're good because we are already the leader
        if self.id.load(Ordering::SeqCst) == inner.leader_id {
            return Ok(response(true));
        }

        // if leader is out of date, do not listen to it
        if inner.term < current_term {
            return Ok(response(false));
        }

        // if AppendEntries rpc received from new leader, convert to follower
        if *self.role.read().await == Role::Candidate {
            let _send_result = self.role_transition_tx.send(Role::Follower).await;
        }

        // reply false if log doesn't contain an entry at prev_log_index whose term matches
        // prev_log_term
        match self.log.get(inner.prev_log_index.try_into().unwrap()).await {
            Ok(Some(log_entry)) => {
                if !log_entry.term == inner.prev_log_term {
                    return Ok(response(false));
                }
            }
            Ok(None) => return Ok(response(false)),
            Err(e) => {
                eprintln!("Error accessing log: {:?}", e);
                return Ok(response(false));
            }
        };

        // clear entries following what the leader says we have
        // TODO this can be more efficient by not blindly removing everything after
        // prev_log_index, and instead verifying what needs to be removed
        while let Ok(Some(_)) = self.log.get(inner.prev_log_index + 1).await {
            let _entry = self.log.pop().await;
        }

        if let Err(e) = self.log.extend(inner.entries).await {
            println!("Error encountered adding entries to log: {}", e);
            return Ok(response(false));
        }

        let last_new_entry_index = self.log.last_index().await;

        // if leader_commit > commit_index, set commit_index = min(leader_commit, index of last
        // new entry)
        if inner.leader_commit > self.commit_index() {
            self.set_commit_index(std::cmp::min(inner.leader_commit, last_new_entry_index));
        }

        Ok(response(true))
    }

    async fn request_vote(
        &self,
        req: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let inner = req.into_inner();
        self.term_check(inner.term).await;
        let current_term = self.current_term();

        let vote = |vote_granted: bool| -> Response<RequestVoteResponse> {
            Response::new(RequestVoteResponse {
                term: current_term,
                vote_granted,
            })
        };

        // requester cannot get vote because they are out of date
        if inner.term < current_term {
            return Ok(vote(false));
        }

        let mut voted_for = self.voted_for.write().await;
        if (voted_for.is_none() || voted_for.unwrap() == inner.candidate_id)
            && (inner.last_log_index >= self.log.last_index().await
                && inner.last_log_term >= self.log.last_term().await)
        {
            voted_for.insert(inner.candidate_id);
            return Ok(vote(true));
        } else {
            return Ok(vote(false));
        }
    }
}
