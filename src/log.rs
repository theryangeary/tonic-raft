use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::consensus::Entry;

#[tonic::async_trait]
// TODO check if LogError should actually be an associated type
/// Trait to define the interface to the Log module
///
/// A Log module implementation must implement [`Default`], and in the process should create the
/// first log at index 0 as an empty Entry with term 0.
///
/// The Log is made up of [`Entry`]s, which have a term number according to the Consensus module's
/// leader's term, and a serialized data payload based on the consuming application.
pub trait Log<Entry, LogError>: Default + std::fmt::Debug + Clone + Send + Sync {
    /// Append log entry to log
    async fn append(&self, entry: Entry) -> Result<(), LogError>;

    /// Get index of last log entry
    async fn last_index(&self) -> u64;

    /// Get term of last log entry
    async fn last_term(&self) -> u64;

    /// Get log entry at index
    ///
    /// Return Ok(None) if no entry exists at that index.
    ///
    /// Return Err(_) if an error occurs evaluating this request.
    async fn get(&self, index: u64) -> Result<Option<Entry>, LogError>;

    /// Remove last log entry
    ///
    /// Raft logs are intended to be write-only in general, but in error conditions different
    /// brokers' logs can become out of sync, so we must remove some log entries based on the
    /// leader's log.
    async fn pop(&self) -> Result<Option<Entry>, LogError>;

    /// Extend log with entries
    async fn extend(&self, entries: Vec<Entry>) -> Result<(), LogError>;
}

#[derive(Debug, Clone)]
pub struct InMemoryLog {
    entries: Arc<RwLock<Vec<Entry>>>,
}

impl InMemoryLog {
    /// Create a new log
    ///
    /// The log should never be empty, and should begin at 1 (based on the raft paper), so an entry
    /// is included for term 0 with an empty data buffer
    pub fn new() -> Self {
        Default::default()
    }
}

#[tonic::async_trait]
// TODO replace String with a proper error type
impl Log<Entry, String> for InMemoryLog {
    async fn append(&self, entry: Entry) -> Result<(), String> {
        self.entries.write().await.push(entry);

        Ok(())
    }
    async fn last_index(&self) -> u64 {
        (self.entries.read().await.len() - 1).try_into().unwrap()
    }

    async fn last_term(&self) -> u64 {
        self.entries
            .read()
            .await
            .last()
            .expect("Log should never be empty")
            .term
    }

    async fn get(&self, index: u64) -> Result<Option<Entry>, String> {
        Ok(self
            .entries
            .read()
            .await
            .get::<usize>(index.try_into().unwrap())
            .map(Clone::clone))
    }

    async fn pop(&self) -> Result<Option<Entry>, String> {
        Ok(self.entries.write().await.pop())
    }

    async fn extend(&self, entries: Vec<Entry>) -> Result<(), String> {
        self.entries.write().await.extend(entries);

        Ok(())
    }
}

impl Default for InMemoryLog {
    fn default() -> Self {
        Self {
            entries: Arc::new(RwLock::new(vec![Entry {
                term: 0,
                data: vec![],
            }])),
        }
    }
}
