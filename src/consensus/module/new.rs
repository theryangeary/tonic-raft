use futures::StreamExt;
use std::{
    future,
    net::SocketAddr,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time::timeout,
};
use tonic::Request;

use crate::{
    consensus::ClusterMember,
    raft::{consensus_client::ConsensusClient, AppendEntriesRequest, RequestVoteRequest},
    ConsensusModule, Entry, Log, Role,
};

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
        let task_handles = Arc::new(RwLock::new(Vec::<JoinHandle<()>>::new()));
        let voted_for = Arc::new(RwLock::new(None));

        let cluster_members = Arc::new(RwLock::new(
            broker_list
                .iter()
                .copied()
                .map(ClusterMember::new)
                .collect(),
        ));

        let consensus_module = Self {
            cluster_members,
            commit_index,
            current_term,
            election_timeout_reset_tx,
            role_transition_tx,
            last_applied: Arc::new(AtomicU64::from(0)),
            log,
            role,
            task_handles,
            voted_for,
            id,
        };

        // s is basically `self` in the _role_transition_task
        let s = consensus_module.clone();

        let _role_transition_task = tokio::spawn(async move {
            while let Some(role) = role_transition_rx.recv().await {
                println!("{:?} becoming {:?}", s.id(), role);
                // close any tasks associated with the previous role
                {
                    let mut task_handles = s.task_handles.write().await;
                    while let Some(task) = task_handles.pop() {
                        task.abort();
                    }
                }

                {
                    // set role
                    let mut r = s.role.write().await;
                    *r = role;
                }

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

                        let votes_needed_to_become_leader = s.num_brokers().await / 2 + 1;

                        let sc = s.clone();
                        // request votes from other servers
                        let votes_received = s
                            .for_each_cluster_member(|cluster_member| async move {
                                // request vote, increment vote count on success
                                let mut client = ConsensusClient::connect(format!(
                                    "http://{}",
                                    cluster_member.socket_address()
                                ))
                                .await
                                .map_err(|e| format!("failed to connect: {:?}", e))?;

                                let request = Request::new(RequestVoteRequest {
                                    term: sc.current_term(),
                                    candidate_id: sc.id(),
                                    last_log_index: sc.log.last_index().await,
                                    last_log_term: sc.log.last_term().await,
                                });

                                client
                                    .request_vote(request)
                                    .await
                                    .map_err(|e| format!("failed to run request_vote RPC: {:?}", e))
                            })
                            .await
                            .filter(|response| match response {
                                Ok(request_vote_response) => {
                                    future::ready(request_vote_response.get_ref().vote_granted)
                                }
                                Err(e) => {
                                    eprintln!("{:?}", e);
                                    future::ready(false)
                                }
                            })
                            .collect::<Vec<_>>()
                            .await
                            .len();

                        println!("broker {} got {} votes", s.id(), votes_received);
                        if votes_received >= votes_needed_to_become_leader {
                            println!(
                                "broker {} becomes the leader for term {}",
                                s.id(),
                                s.current_term()
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
                                let sc = s2.clone();

                                let mut stream = s2
                                    .for_each_cluster_member(|cluster_member| async move {
                                        let mut client = ConsensusClient::connect(format!(
                                            "http://{}",
                                            cluster_member
                                        ))
                                        .await
                                        .map_err(|e| format!("failed to connect; {:?}", e))?;

                                        let request = {
                                            Request::new(AppendEntriesRequest {
                                                term: sc.current_term(),
                                                leader_id: sc.id(),
                                                prev_log_index: sc.log.last_index().await,
                                                prev_log_term: sc.log.last_term().await,
                                                entries: vec![],
                                                leader_commit: sc.commit_index(),
                                            })
                                        };

                                        return match client.append_entries(request).await {
                                            Ok(_) => Ok(()),
                                            Err(response_error) => Err(response_error.to_string()),
                                        };
                                    })
                                    .await;

                                while let Some(result) = stream.next().await {
                                    if let Err(e) = result {
                                        eprintln!("{:?}", e);
                                    }
                                }

                                interval.tick().await;
                            }
                        });

                        // yield now to promote starting the heartbeat task sooner, and reducing
                        // the likelihood of leader contention and producing multiple elections
                        // back to back
                        tokio::task::yield_now().await;

                        {
                            s.task_handles.write().await.push(heartbeat_task);
                        }

                        // if last log index > nextIndex for a follower, send AppendEntries rpc
                        // TODO reinstate append entries functionality, preferably in an event
                        // driven fashion
                        //let s2 = s.clone();
                        //let append_entries_task = tokio::spawn(async move {
                        //println!("starting appending entries");
                        //// initialize leader state
                        //{
                        //let broker_list = s2.brokers.read().await;
                        //let last_index = s2.log.last_index().await;
                        //// initialize next_index list to leader's last log index + 1
                        //let mut next_index_list = s2.next_index.write().await;
                        //for broker in &*broker_list {
                        //let _old_value =
                        //next_index_list.insert(broker.clone(), last_index + 1);
                        //}

                        //// initialize match_index list to 0
                        //let mut match_index_list = s2.match_index.write().await;
                        //for broker in &*broker_list {
                        //let _old_value = match_index_list.insert(broker.clone(), 0);
                        //}
                        //}

                        //loop {
                        //// TODO this is a busy loop, churning through CPU
                        //let last_log_index = s2.log.last_index().await;

                        //for (broker_socket_addr, next_index) in
                        //s2.next_index.write().await.iter_mut()
                        //{
                        //if last_log_index >= *next_index {
                        //println!(
                        //"sending to broker_socket_addr {}",
                        //broker_socket_addr
                        //);
                        //let mut client = ConsensusClient::connect(format!(
                        //"http://{}",
                        //broker_socket_addr
                        //))
                        //.await
                        //.unwrap();

                        //let entries = s2.log.get_from(*next_index).await.unwrap();
                        //let entries_len = entries.len() as u64;

                        //let prev_log_index = next_index.saturating_sub(1);
                        //let prev_log_term = s2
                        //.log
                        //.get(prev_log_index)
                        //.await
                        //.expect("Error accessing log")
                        //.expect("There shoulda been a log at this index")
                        //.term;

                        //let request = Request::new(AppendEntriesRequest {
                        //term: s2.current_term(),
                        //leader_id: s2.id(),
                        //prev_log_index,
                        //prev_log_term,
                        //entries,
                        //leader_commit: s2.commit_index(),
                        //});

                        //let inner = client
                        //.append_entries(request)
                        //.await
                        //.unwrap()
                        //.into_inner();

                        //if inner.success {
                        //*next_index += entries_len;
                        //s2.match_index.write().await.insert(
                        //*broker_socket_addr,
                        //next_index.saturating_sub(1),
                        //);
                        //} else {
                        //*next_index = next_index.saturating_sub(1);
                        //}
                        //}
                        //}

                        //tokio::task::yield_now().await;
                        //}
                        //});

                        //{
                        //s.task_handles.write().await.push(append_entries_task);
                        //}

                        // if there exists an N such that N > commit_index, a majority of
                        // match_index[i] >= N, and Log[N].term == current_term, set commit_index =
                        // N

                        // TODO reinstate update commit index task, preferably in an event
                        // oriented way instead
                        //let s2 = s.clone();
                        //let update_commit_index_task = tokio::spawn(async move {
                        //println!("starting updating commit_index");
                        //let majority_count = { s2.brokers.read().await.len() / 2 + 1 };

                        //loop {
                        //// TODO this is busy loop, churning CPU
                        //let n = s2.commit_index() + 1;
                        //let num_matching = s2
                        //.match_index
                        //.read()
                        //.await
                        //.values()
                        //.filter(|match_index| **match_index >= n)
                        //.count();

                        //let log_term = if let Ok(Some(entry)) = s2.log.get(n).await {
                        //entry.term
                        //} else {
                        //continue;
                        //};

                        //if num_matching >= majority_count && log_term == s2.current_term() {
                        //s2.set_commit_index(n);
                        //}

                        //tokio::task::yield_now().await;
                        //}
                        //});

                        //{
                        //s.task_handles.write().await.push(update_commit_index_task);
                        //}
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
