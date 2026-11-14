//! Shared in-process latest recovery value, without file or history semantics.
use std::sync::{Arc, Mutex};

use super::{Journal, Recover};
use crate::Error;

#[derive(Clone, Default)]
pub struct MemoryJournal {
    latest: Arc<Mutex<Option<Recover>>>,
}

impl MemoryJournal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn latest(&self) -> Option<Recover> {
        self.latest
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn store(&self, recover: Recover) -> Result<(), Error> {
        recover.validate()?;
        *self.latest.lock().unwrap_or_else(|e| e.into_inner()) = Some(recover);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Journal for MemoryJournal {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        self.store(recover)
    }

    async fn has_selected_history(&self) -> bool {
        self.latest().is_some()
    }

    async fn recovered(&self, recover: Recover) -> Result<(), Error> {
        self.store(recover)
    }
}
