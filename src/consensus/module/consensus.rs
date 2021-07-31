use std::convert::TryInto;
use std::sync::atomic::Ordering;
use tonic::{Request, Response, Status};

use crate::raft::consensus_server::Consensus;
use crate::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::{ConsensusModule, Entry, Log, Role};

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
            if let Err(e) = self.role_transition_tx.send(Role::Follower).await {
                eprintln!("Failed to send role transition message: {:?}", e);
            }
        }

        // reset election timeout
        self.reset_election_timeout().await.map_err(|e| {
            Status::internal(format!(
                "Append entries RPC failed to reset election timeout: {:?}",
                e
            ))
        })?;

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

        if inner.entries.len() > 0 {
            println!("Appending {:?}", inner.entries);
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
            self.reset_election_timeout().await.map_err(|e| {
                Status::internal(format!(
                    "Request vote RPC failed to reset election timeout: {:?}",
                    e
                ))
            })?;
            println!(
                "granting vote for term {} to {}",
                current_term, inner.candidate_id
            );
            return Ok(vote(true));
        } else {
            return Ok(vote(false));
        }
    }
}
