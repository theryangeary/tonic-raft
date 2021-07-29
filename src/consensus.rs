use crate::entry_appender::EntryAppender;
use crate::log::Log;
use crate::log::Transition;
use raft::consensus_client::ConsensusClient;
use raft::consensus_server::Consensus;
use raft::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
use std::collections::HashMap;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tonic::{Request, Response, Status};

pub use raft::consensus_server::ConsensusServer;
pub use raft::Entry;

pub mod raft {
    tonic::include_proto!("raft");
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
    pub fn new(id: u64, broker_list: Vec<SocketAddr>) -> Self {
        let (role_transition_tx, mut role_transition_rx) = mpsc::channel(1);
        let (election_timeout_reset_tx, _) = broadcast::channel(1);
        let (log_entry_tx, _) = mpsc::channel(1);
        let (log_entry_tx_watch_tx, log_entry_tx_watch_rx) = watch::channel(log_entry_tx);
        let log_entry_tx_watch_tx = Arc::new(log_entry_tx_watch_tx);
        let current_term = Arc::new(AtomicU64::new(0));
        let id = Arc::new(AtomicU64::new(id));
        let log = L::default();
        let role = Arc::new(RwLock::new(Role::Follower));
        let commit_index = Arc::new(AtomicU64::new(0));
        let brokers = Arc::new(RwLock::new(broker_list));
        let next_index = Arc::new(RwLock::new(HashMap::new()));
        let match_index = Arc::new(RwLock::new(HashMap::new()));
        let task_handles = Arc::new(RwLock::new(Vec::<JoinHandle<()>>::new()));

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
        let match_index_handle = Arc::clone(&match_index);
        let task_handles_handle = Arc::clone(&task_handles);
        let log_entry_tx_watch_tx_handle = Arc::clone(&log_entry_tx_watch_tx);

        let _role_transition_task = tokio::spawn(async move {
            while let Some(role) = role_transition_rx.recv().await {
                println!("{:?} becoming {:?}", id_handle.load(Ordering::SeqCst), role);
                // close any tasks associated with the previous role
                {
                    let mut task_handles = task_handles_handle.write().await;
                    while let Some(task) = task_handles.pop() {
                        task.abort();
                    }
                }

                let role_transition_tx = role_transition_tx2.clone();
                {
                    let mut r = role_handle.write().await;
                    *r = role;
                }

                match role {
                    Role::Follower => {
                        // set thread to handle election timeout
                        let mut election_timeout_reset_rx = election_timeout_reset_tx2.subscribe();
                        let election_timeout_task = tokio::spawn(async move {
                            if let Err(_elapsed) = timeout(
                                Duration::from_millis(150u64 + (rand::random::<u64>() % 150u64)),
                                election_timeout_reset_rx.recv(),
                            )
                            .await
                            {
                                println!("timeout!");
                                let _send_result = role_transition_tx.send(Role::Candidate).await;
                            }
                        });
                        task_handles_handle
                            .write()
                            .await
                            .push(election_timeout_task);
                    }
                    Role::Candidate => {
                        let old_term = current_term_handle.load(Ordering::SeqCst);
                        let new_term = old_term + 1;
                        current_term_handle.store(new_term, Ordering::SeqCst);

                        let broker_list = brokers_handle.read().await;
                        let votes_needed_to_become_leader = broker_list.len() / 2 + 1;
                        let mut votes = 0;
                        // TODO make these requests in parallel and short circuit on majority
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
                        println!(
                            "broker {} got {} votes",
                            id_handle.load(Ordering::SeqCst),
                            votes
                        );
                        if votes >= votes_needed_to_become_leader {
                            println!(
                                "broker {} becomes the leader",
                                id_handle.load(Ordering::SeqCst)
                            );
                            let _send_result = role_transition_tx.send(Role::Leader).await;
                        } else {
                            let _send_result = role_transition_tx.send(Role::Follower).await;
                        }
                    }
                    Role::Leader => {
                        let id = Arc::clone(&id_handle);
                        let commit_index = Arc::clone(&commit_index_handle);
                        let current_term = Arc::clone(&current_term_handle);
                        let next_index_handle = Arc::clone(&next_index_handle);
                        let match_index = Arc::clone(&match_index_handle);
                        let log_handle1 = log_handle.clone();

                        let mut task_handles = task_handles_handle.write().await;

                        // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each
                        // server; repeat (//TODO only) during idle periods to prevent election timeouts
                        let heartbeat_broker_list = Arc::clone(&brokers_handle);

                        let heartbeat_task = tokio::spawn(async move {
                            println!("starting heartbeat");
                            let mut interval =
                                tokio::time::interval(tokio::time::Duration::from_millis(100));

                            loop {
                                let prev_log_index = log_handle1.last_index().await;
                                let prev_log_term = log_handle1.last_term().await;

                                for broker_socket_addr in &*heartbeat_broker_list.read().await {
                                    let mut client = ConsensusClient::connect(format!(
                                        "http://{}",
                                        broker_socket_addr
                                    ))
                                    .await
                                    .unwrap();

                                    let request = {
                                        Request::new(AppendEntriesRequest {
                                            term: current_term.load(Ordering::SeqCst),
                                            leader_id: id.load(Ordering::SeqCst),
                                            prev_log_index,
                                            prev_log_term,
                                            entries: vec![],
                                            leader_commit: commit_index.load(Ordering::SeqCst),
                                        })
                                    };

                                    if let Err(response_error) =
                                        client.append_entries(request).await
                                    {
                                        if response_error.code() != tonic::Code::Cancelled {
                                            eprintln!("Error: {:?}", response_error);
                                        }
                                    }
                                }

                                interval.tick().await;
                            }
                        });

                        task_handles.push(heartbeat_task);

                        // If command received from client: append entry to local log, respond
                        let log_entry_tx_watch_tx_handle =
                            Arc::clone(&log_entry_tx_watch_tx_handle);
                        let log_handle2 = log_handle.clone();
                        let commit_index = Arc::clone(&commit_index_handle);
                        let service_task = tokio::spawn(async move {
                            println!("starting servicing client");
                            let (log_entry_tx, mut log_entry_rx) = mpsc::channel(1);
                            let _send_result = log_entry_tx_watch_tx_handle
                                .send(log_entry_tx)
                                .expect("Failed to send log_entry_tx");

                            while let Some(entry) = log_entry_rx.recv().await {
                                println!("recv'd entry: {:?}", entry);
                                // append to log
                                let index = log_handle2.append(entry).await.unwrap();
                                // TODO replicate to a majority of logs
                                // for now by just waiting until commit_index is at least as high
                                // as this log's index
                                while index > commit_index.load(Ordering::SeqCst) {
                                    tokio::task::yield_now().await;
                                }
                                // TODO apply to state machine
                            }
                        });

                        task_handles.push(service_task);

                        // if last log index > nextIndex for a follower, send AppendEntries rpc
                        let append_entries_broker_list = Arc::clone(&brokers_handle);
                        let log_handle3 = log_handle.clone();
                        let id = Arc::clone(&id_handle);
                        let commit_index = Arc::clone(&commit_index_handle);
                        let current_term = Arc::clone(&current_term_handle);
                        let next_index_handle = Arc::clone(&next_index_handle);
                        let match_indexes = Arc::clone(&match_index_handle);

                        let append_entries_task = tokio::spawn(async move {
                            println!("starting appending entries");
                            // initialize leader state
                            {
                                let broker_list = append_entries_broker_list.read().await;
                                let last_index = log_handle3.last_index().await;
                                // initialize next_index list to leader's last log index + 1
                                let mut next_index_list = next_index_handle.write().await;
                                for broker in &*broker_list {
                                    let _old_value =
                                        next_index_list.insert(broker.clone(), last_index + 1);
                                }

                                // initialize match_index list to 0
                                let mut match_index_list = match_index.write().await;
                                for broker in &*broker_list {
                                    let _old_value = match_index_list.insert(broker.clone(), 0);
                                }
                            }

                            loop {
                                // TODO this is a busy loop, churning through CPU
                                let last_log_index = log_handle3.last_index().await;

                                for (broker_socket_addr, next_index) in
                                    next_index_handle.write().await.iter_mut()
                                {
                                    if last_log_index >= *next_index {
                                        let mut client = ConsensusClient::connect(format!(
                                            "http://{}",
                                            broker_socket_addr
                                        ))
                                        .await
                                        .unwrap();

                                        let entries =
                                            log_handle3.get_from(*next_index).await.unwrap();
                                        let entries_len = entries.len() as u64;

                                        let prev_log_index = next_index.saturating_sub(1);
                                        let prev_log_term = log_handle3
                                            .get(prev_log_index)
                                            .await
                                            .expect("Error accessing log")
                                            .expect("There shoulda been a log at this index")
                                            .term;

                                        let request = Request::new(AppendEntriesRequest {
                                            term: current_term.load(Ordering::SeqCst),
                                            leader_id: id.load(Ordering::SeqCst),
                                            prev_log_index,
                                            prev_log_term,
                                            entries,
                                            leader_commit: commit_index.load(Ordering::SeqCst),
                                        });

                                        let inner = client
                                            .append_entries(request)
                                            .await
                                            .unwrap()
                                            .into_inner();

                                        if inner.success {
                                            *next_index += entries_len;
                                            match_indexes.write().await.insert(
                                                *broker_socket_addr,
                                                next_index.saturating_sub(1),
                                            );
                                        } else {
                                            *next_index = next_index.saturating_sub(1);
                                        }
                                    }
                                }
                            }
                        });

                        task_handles.push(append_entries_task);

                        // if there exists an N such that N > commit_index, a majority of
                        // match_index[i] >= N, and Log[N].term == current_term, set commit_index =
                        // N
                        let commit_index = Arc::clone(&commit_index_handle);
                        let update_commit_index_broker_list = Arc::clone(&brokers_handle);
                        let match_indexes = Arc::clone(&match_index_handle);
                        let log_handle4 = log_handle.clone();
                        let current_term = Arc::clone(&current_term_handle);

                        let update_commit_index_task = tokio::spawn(async move {
                            println!("starting updating commit_index");
                            let majority_count =
                                { update_commit_index_broker_list.read().await.len() / 2 + 1 };

                            loop {
                                let n = commit_index.load(Ordering::SeqCst) + 1;
                                let num_matching = match_indexes
                                    .read()
                                    .await
                                    .values()
                                    .filter(|match_index| **match_index >= n)
                                    .count();

                                let log_term = if let Ok(Some(entry)) = log_handle4.get(n).await {
                                    entry.term
                                } else {
                                    continue;
                                };

                                if num_matching >= majority_count
                                    && log_term == current_term.load(Ordering::SeqCst)
                                {
                                    commit_index.store(n, Ordering::SeqCst);
                                }
                            }
                        });

                        task_handles.push(update_commit_index_task);
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
            last_applied: Arc::new(AtomicU64::from(0)),
            log,
            match_index,
            next_index,
            role,
            task_handles,
            voted_for: Arc::new(RwLock::new(None)),
            id,
            log_entry_tx_watch_tx,
            log_entry_tx_watch_rx,
        }
    }

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

    /// Get EntryAppender, for leader to append logs with
    pub fn entry_appender(&self) -> EntryAppender {
        EntryAppender::new(
            Arc::clone(&self.current_term),
            self.log_entry_tx_watch_rx.clone(),
        )
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

        // reset election timeout
        if *self.role.read().await == Role::Follower
            && self.election_timeout_reset_tx.receiver_count() > 0
        {
            self.election_timeout_reset_tx.send(()).unwrap();
        }

        // reply false if log doesn't contain an entry at prev_log_index whose term matches
        // prev_log_term
        match self.log.get(inner.prev_log_index.try_into().unwrap()).await {
            Ok(Some(log_entry)) => {
                if !log_entry.term == inner.prev_log_term {
                    return Ok(response(false));
                }
            }
            Ok(None) => {
                return Ok(response(false));
            }
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
