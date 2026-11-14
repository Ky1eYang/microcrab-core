//! In-process content-addressed backend; serialized operations never suspend.
use super::{
    CreatorEnvelope, History, HistoryNode, JournalCursor, RestorePoint, RestoreTarget, Travel,
    canonical_bytes,
};
use microcrab_session::Error;
use std::{collections::HashMap, sync::Mutex};

#[derive(Default)]
pub struct MemoryJournal {
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    head: Option<JournalCursor>,
    records: HashMap<String, HistoryNode>,
    redo: Vec<String>,
}
impl MemoryJournal {
    pub fn new() -> Self {
        Self::default()
    }
}
fn conflict() -> Error {
    Error::Protocol("journal cursor conflict".into())
}
fn missing() -> Error {
    Error::config("journal target does not exist")
}
impl State {
    fn check(&self, expected: &Option<JournalCursor>) -> Result<(), Error> {
        if &self.head != expected {
            return Err(conflict());
        }
        Ok(())
    }
    fn point(&self, id: &str) -> Result<RestorePoint, Error> {
        let node = self.records.get(id).ok_or_else(missing)?;
        node.validate(id)?;
        Ok(RestorePoint {
            parent: node.parent.clone(),
            cursor: node.cursor(id.into()),
            checkpoint: node.checkpoint.clone(),
        })
    }
    fn target(&self, via: &Travel) -> Result<String, Error> {
        match via {
            Travel::Undo => self
                .records
                .get(&self.head.as_ref().ok_or_else(missing)?.checkpoint_id)
                .and_then(|node| node.parent.clone())
                .ok_or_else(missing),
            Travel::Redo => self.redo.last().cloned().ok_or_else(missing),
            Travel::To { checkpoint_id } | Travel::UndoTo { checkpoint_id } => {
                self.point(checkpoint_id)?;
                Ok(checkpoint_id.clone())
            }
        }
    }
}
impl History for MemoryJournal {
    async fn commit(
        &self,
        expected: Option<JournalCursor>,
        checkpoint: CreatorEnvelope,
    ) -> Result<JournalCursor, Error> {
        checkpoint.validate()?;
        let mut state = self.state.lock().map_err(|_| conflict())?;
        state.check(&expected)?;
        if let Some(head) = &expected {
            if head.session_id != checkpoint.session_id {
                return Err(conflict());
            }
            if canonical_bytes(&state.point(&head.checkpoint_id)?.checkpoint)?
                == canonical_bytes(&checkpoint)?
            {
                return Ok(head.clone());
            }
        }
        let node = HistoryNode::new(expected.map(|head| head.checkpoint_id), checkpoint);
        let id = node.id()?;
        let cursor = node.cursor(id.clone());
        state.records.insert(id, node);
        state.redo.clear();
        state.head = Some(cursor.clone());
        Ok(cursor)
    }
    async fn restore(&self, target: RestoreTarget) -> Result<RestorePoint, Error> {
        let state = self.state.lock().map_err(|_| conflict())?;
        let id = match target {
            RestoreTarget::Head => state
                .head
                .as_ref()
                .ok_or_else(missing)?
                .checkpoint_id
                .clone(),
            RestoreTarget::Checkpoint { checkpoint_id } => checkpoint_id,
        };
        state.point(&id)
    }
    async fn travel(&self, expected: JournalCursor, via: Travel) -> Result<RestorePoint, Error> {
        let state = self.state.lock().map_err(|_| conflict())?;
        state.check(&Some(expected))?;
        state.point(&state.target(&via)?)
    }
    async fn move_head(
        &self,
        expected: JournalCursor,
        target: JournalCursor,
        via: Travel,
    ) -> Result<JournalCursor, Error> {
        let mut state = self.state.lock().map_err(|_| conflict())?;
        state.check(&Some(expected.clone()))?;
        if state.point(&state.target(&via)?)?.cursor != target {
            return Err(conflict());
        }
        match via {
            Travel::Undo | Travel::UndoTo { .. } => state.redo.push(expected.checkpoint_id),
            Travel::Redo => {
                state.redo.pop();
            }
            Travel::To { .. } => state.redo.clear(),
        }
        state.head = Some(target.clone());
        Ok(target)
    }
}
