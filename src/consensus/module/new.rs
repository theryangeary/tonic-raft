use std::collections::HashMap;
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
use tonic::Request;

use crate::raft::consensus_client::ConsensusClient;
use crate::raft::{AppendEntriesRequest, RequestVoteRequest};
use crate::{ConsensusModule, Entry, Log, Role};

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
        let next_index = Arc::new(RwLock::new(HashMap::new()));
        let match_index = Arc::new(RwLock::new(HashMap::new()));
        let task_handles = Arc::new(RwLock::new(Vec::<JoinHandle<()>>::new()));
        let voted_for = Arc::new(RwLock::new(None));

        let consensus_module = Self {
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
            voted_for,
            id,
        };

        // s is basically `self` in the _role_transition_task
        let s = consensus_module.clone();

        let _role_transition_task = tokio::spawn(async move {
            while let Some(role) = role_transition_rx.recv().await {
                println!("{:?} becoming {:?}", s.id.load(Ordering::SeqCst), role);
                // close any tasks associated with the previous role
                {
                    let mut task_handles = s.task_handles.write().await;
                    while let Some(task) = task_handles.pop() {
                        task.abort();
                    }
                }

                s.set_role(role).await;

                match role {
                    Role::Follower => {
                        // set thread to handle election timeout
                        let mut election_timeout_reset_rx = s.election_timeout_reset_tx.subscribe();
                        let s2 = s.clone();
                        let election_timeout_task = tokio::spawn(async move {
                            loop {
                                if let Err(_elapsed) = timeout(
                                    Duration::from_millis(
                                        150u64 + (rand::random::<u64>() % 150u64),
                                    ),
                                    election_timeout_reset_rx.recv(),
                                )
                                .await
                                {
                                    println!("timeout!");
                                    let _send_result = s2.become_role(Role::Candidate).await;
                                }
                            }
                        });
                        s.task_handles.write().await.push(election_timeout_task);
                    }
                    Role::Candidate => {
                        // start election

                        // increment current_term
                        s.increment_current_term().await;

                        // request votes from other servers
                        let broker_list = s.brokers.read().await;
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
                                term: s.current_term.load(Ordering::SeqCst),
                                candidate_id: s.id.load(Ordering::SeqCst),
                                last_log_index: s.log.last_index().await,
                                last_log_term: s.log.last_term().await,
                            });

                            let response = client.request_vote(request).await.unwrap();
                            let inner = response.into_inner();
                            if inner.vote_granted {
                                votes += 1;
                            }
                        }
                        println!("broker {} got {} votes", s.id.load(Ordering::SeqCst), votes);
                        if votes >= votes_needed_to_become_leader {
                            println!(
                                "broker {} becomes the leader for term {}",
                                s.id.load(Ordering::SeqCst),
                                s.current_term.load(Ordering::SeqCst)
                            );
                            if let Err(e) = s.become_role(Role::Leader).await {
                                eprintln!("failed to send role transition request: {:?}", e);
                            }
                        } else {
                            if let Err(e) = s.become_role(Role::Follower).await {
                                eprintln!("failed to send role transition request: {:?}", e);
                            }
                        }
                    }
                    Role::Leader => {
                        // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each
                        // server; repeat (//TODO only) during idle periods to prevent election timeouts

                        let s2 = s.clone();
                        let heartbeat_task = tokio::spawn(async move {
                            println!("starting heartbeat");
                            let mut interval =
                                tokio::time::interval(tokio::time::Duration::from_millis(100));

                            loop {
                                let prev_log_index = s2.log.last_index().await;
                                let prev_log_term = s2.log.last_term().await;

                                for broker_socket_addr in &*s2.brokers.read().await {
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
                                            term: s2.current_term.load(Ordering::SeqCst),
                                            leader_id: s2.id.load(Ordering::SeqCst),
                                            prev_log_index,
                                            prev_log_term,
                                            entries: vec![],
                                            leader_commit: s2.commit_index.load(Ordering::SeqCst),
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

                        {
                            s.task_handles.write().await.push(heartbeat_task);
                        }

                        // if last log index > nextIndex for a follower, send AppendEntries rpc
                        let s2 = s.clone();
                        let append_entries_task = tokio::spawn(async move {
                            println!("starting appending entries");
                            // initialize leader state
                            {
                                let broker_list = s2.brokers.read().await;
                                let last_index = s2.log.last_index().await;
                                // initialize next_index list to leader's last log index + 1
                                let mut next_index_list = s2.next_index.write().await;
                                for broker in &*broker_list {
                                    let _old_value =
                                        next_index_list.insert(broker.clone(), last_index + 1);
                                }

                                // initialize match_index list to 0
                                let mut match_index_list = s2.match_index.write().await;
                                for broker in &*broker_list {
                                    let _old_value = match_index_list.insert(broker.clone(), 0);
                                }
                            }

                            loop {
                                // TODO this is a busy loop, churning through CPU
                                let last_log_index = s2.log.last_index().await;

                                for (broker_socket_addr, next_index) in
                                    s2.next_index.write().await.iter_mut()
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

                                        let entries = s2.log.get_from(*next_index).await.unwrap();
                                        let entries_len = entries.len() as u64;

                                        let prev_log_index = next_index.saturating_sub(1);
                                        let prev_log_term = s2
                                            .log
                                            .get(prev_log_index)
                                            .await
                                            .expect("Error accessing log")
                                            .expect("There shoulda been a log at this index")
                                            .term;

                                        let request = Request::new(AppendEntriesRequest {
                                            term: s2.current_term(),
                                            leader_id: s2.id(),
                                            prev_log_index,
                                            prev_log_term,
                                            entries,
                                            leader_commit: s2.commit_index(),
                                        });

                                        let inner = client
                                            .append_entries(request)
                                            .await
                                            .unwrap()
                                            .into_inner();

                                        if inner.success {
                                            *next_index += entries_len;
                                            s2.match_index.write().await.insert(
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

                        {
                            s.task_handles.write().await.push(append_entries_task);
                        }

                        // if there exists an N such that N > commit_index, a majority of
                        // match_index[i] >= N, and Log[N].term == current_term, set commit_index =
                        // N

                        let s2 = s.clone();
                        let update_commit_index_task = tokio::spawn(async move {
                            println!("starting updating commit_index");
                            let majority_count = { s2.brokers.read().await.len() / 2 + 1 };

                            loop {
                                // TODO this is busy loop, churning CPU
                                let n = s2.commit_index.load(Ordering::SeqCst) + 1;
                                let num_matching = s2
                                    .match_index
                                    .read()
                                    .await
                                    .values()
                                    .filter(|match_index| **match_index >= n)
                                    .count();

                                let log_term = if let Ok(Some(entry)) = s2.log.get(n).await {
                                    entry.term
                                } else {
                                    continue;
                                };

                                if num_matching >= majority_count
                                    && log_term == s2.current_term.load(Ordering::SeqCst)
                                {
                                    s2.set_commit_index(n);
                                }

                                tokio::task::yield_now().await;
                            }
                        });

                        {
                            s.task_handles.write().await.push(update_commit_index_task);
                        }
                    }
                }
            }
        });

        let cm = consensus_module.clone();
        // start the server off as a follower
        tokio::spawn(async move { cm.become_role(Role::Follower).await });

        consensus_module
    }
}
