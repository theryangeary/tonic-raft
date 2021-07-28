use serde::Serialize;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::{mpsc, watch};

use crate::consensus::Entry;

#[derive(Clone)]
pub struct EntryAppender {
    current_term: Arc<AtomicU64>,
    log_entry_tx_watch_rx: watch::Receiver<mpsc::Sender<Entry>>,
}

impl EntryAppender {
    pub fn new(
        current_term: Arc<AtomicU64>,
        log_entry_tx_watch_rx: watch::Receiver<mpsc::Sender<Entry>>,
    ) -> Self {
        Self {
            current_term,
            log_entry_tx_watch_rx,
        }
    }

    pub async fn append_entry<T>(&mut self, entry: &T) -> Result<(), String>
    where
        T: Serialize,
    {
        let entry_tx = self.log_entry_tx_watch_rx.borrow_and_update().clone();

        let data = bincode::serialize(entry).expect("Failed to encode binary data");

        entry_tx
            .send(Entry {
                data,
                term: self.current_term.load(Ordering::SeqCst),
            })
            .await
            .map_err(|e| format!("Failed to append entry: {:?}", e))
    }
}
