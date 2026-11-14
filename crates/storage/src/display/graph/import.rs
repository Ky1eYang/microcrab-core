//! Fork certificates import sealed graph metadata, while source pointer records
//! retain the exact immutable body references. Imported nodes keep their IDs.
use super::*;
use std::collections::BTreeSet;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportedBlock {
    block: DisplayBlock,
    content_bytes: u64,
    identity_scope: DisplayScope,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Import {
    source_scope: DisplayScope,
    source_high_water: u64,
    legacy_content_records: u64,
    projection_incomplete: bool,
    selected_head: Option<String>,
    blocks: Vec<ImportedBlock>,
}
fn certificate_hash(import: &Import) -> Result<String, Error> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(import)?)))
}
impl DisplayGraph {
    /// Call only on a source graph reconstructed at the fork's pinned watermark.
    /// Source bodies remain referenced by the existing exact fork-record seals.
    pub fn import_event(&self) -> Result<serde_json::Value, Error> {
        if !self.selection_known {
            return Err(failure("display fork historical ordering is unavailable"));
        }
        if self.pending_selection.is_some() {
            return Err(failure("display fork selection is unresolved"));
        }
        let mut blocks = Vec::new();
        let mut cursor = self.selected_head.as_ref();
        while let Some(id) = cursor {
            let block = self
                .blocks
                .get(id)
                .ok_or_else(|| failure("display fork ancestry missing"))?;
            blocks.push(ImportedBlock {
                block: block.clone(),
                content_bytes: self.lengths.get(id).copied().unwrap_or(0),
                identity_scope: self.origins.get(id).unwrap_or(&self.scope).clone(),
            });
            cursor = block.parent_block_id.as_ref();
        }
        blocks.reverse();
        let import = Import {
            source_scope: self.scope.clone(),
            source_high_water: self.high_water,
            legacy_content_records: self.legacy_content_records,
            projection_incomplete: self.projection_incomplete,
            selected_head: self.selected_head.clone(),
            blocks,
        };
        Ok(
            serde_json::json!({"phase":"display_graph_import", "display_graph_version":1, "source_graph_hash": certificate_hash(&import)?, "import":import}),
        )
    }
    pub(super) fn validate_import(&self, event: &serde_json::Value) -> Result<(), Error> {
        if !self.blocks.is_empty() || self.selected_head.is_some() {
            return Err(failure("display fork graph destination is not empty"));
        }
        let import: Import = serde_json::from_value(event["import"].clone())?;
        import.source_scope.validate()?;
        if self
            .legacy_content_records
            .checked_add(import.legacy_content_records)
            .is_none()
        {
            return Err(failure("display fork legacy record count exhausted"));
        }
        if import.source_scope.owner != self.scope.owner
            || import
                .blocks
                .iter()
                .any(|item| item.identity_scope.owner != self.scope.owner)
        {
            return Err(failure("display fork graph owner mismatch"));
        }
        if event["source_graph_hash"].as_str() != Some(certificate_hash(&import)?.as_str()) {
            return Err(failure("display fork graph certificate hash mismatch"));
        }
        let mut known = BTreeSet::new();
        let mut parent = None;
        for item in &import.blocks {
            item.identity_scope.validate()?;
            let block = &item.block;
            let origin = DisplayGraph::empty(item.identity_scope.clone());
            if block.block_id != origin.identity(block)?
                || block.parent_block_id != parent
                || block.content_ref != format!("display:{}", block.block_id)
                || !known.insert(block.block_id.clone())
                || block.sequence == 0
                || block.block_type.is_empty()
                || block.attempt_id.is_empty()
            {
                return Err(failure("display fork graph identity/ancestry mismatch"));
            }
            parent = Some(block.block_id.clone());
        }
        if parent != import.selected_head {
            return Err(failure("display fork graph selected head mismatch"));
        }
        Ok(())
    }
    pub(super) fn apply_import(&mut self, event: &serde_json::Value) -> Result<(), Error> {
        let import: Import = serde_json::from_value(event["import"].clone())?;
        for item in import.blocks {
            self.lengths
                .insert(item.block.block_id.clone(), item.content_bytes);
            self.origins
                .insert(item.block.block_id.clone(), item.identity_scope);
            self.blocks.insert(item.block.block_id.clone(), item.block);
        }
        self.selected_head = import.selected_head;
        self.projection_incomplete |= import.projection_incomplete;
        self.legacy_content_records = self
            .legacy_content_records
            .checked_add(import.legacy_content_records)
            .ok_or_else(|| failure("display fork legacy record count exhausted"))?;
        self.active = None;
        Ok(())
    }
}
