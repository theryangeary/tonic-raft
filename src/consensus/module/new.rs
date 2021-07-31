use std::collections::HashMap;
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
use tonic::Request;

use crate::{Role, Log, Entry, ConsensusModule};
use crate::raft::consensus_client::ConsensusClient;
use crate::raft::{AppendEntriesRequest, RequestVoteRequest};

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
                            let client_result =
                                ConsensusClient::connect(format!("http://{}", broker)).await;

                            let mut client = if let Err(e) = client_result {
                                eprintln!("failed to connect to broker: {:?}", e);
                                continue;
                            } else {
                                client_result.unwrap()
                            };

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
                            if let Err(e) = role_transition_tx.send(Role::Leader).await {
                                eprintln!("failed to send role transition request: {:?}", e);
                            }
                        } else {
                            if let Err(e) = role_transition_tx.send(Role::Follower).await {
                                eprintln!("failed to send role transition request: {:?}", e);
                            }
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
                                    let client_result = ConsensusClient::connect(format!(
                                        "http://{}",
                                        broker_socket_addr
                                    ))
                                    .await;

                                    let mut client = if let Err(e) = client_result {
                                        eprintln!("failed to connect to broker: {:?}", e);
                                        continue;
                                    } else {
                                        client_result.unwrap()
                                    };

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
                                        println!(
                                            "sending to broker_socket_addr {}",
                                            broker_socket_addr
                                        );
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

                                tokio::task::yield_now().await;
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
                                // TODO this is busy loop, churning CPU
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

                                tokio::task::yield_now().await;
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
}
