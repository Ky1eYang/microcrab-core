//! Runtime import handler for `microcrab:session/references@0.3.0`.
//! Stage binary or text references created during a WASM tool call and commit
//! them to the Session's Personal Content Library only after settled success.
use super::RuntimeState;
use crate::admission::image::MAX_IMAGE_BYTES;
use crate::runtime::tool::ToolContext;
use crate::state::reference::{
    CreateLibraryObject, LibraryObjectSummary, LocalBlobOrigin, inspect_binary_for_storage,
};
use crate::{Error, LimitKind};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use wasmtime::component::{Linker, types::ComponentItem};
use wasmtime::{Engine, StoreContextMut};

pub(super) const INTERFACE: &str = "microcrab:session/references@0.3.0";

const MAX_STAGED_OBJECTS_PER_CALL: usize = 16;

#[derive(Clone)]
struct StagedEntry {
    id: String,
    title: String,
    media_type: String,
    data: Vec<u8>,
}

#[derive(Clone)]
pub(super) struct Ingress {
    state: Arc<Mutex<IngressState>>,
}

struct IngressState {
    entries: Vec<StagedEntry>,
    used_bytes: u64,
    used_entries: u64,
    max_bytes: u64,
    max_entries: u64,
}

pub(super) struct StagedReferences {
    entries: Vec<StagedEntry>,
}

impl Ingress {
    pub(super) async fn new(context: &ToolContext, cancel: &CancellationToken) -> Result<Self, Error> {
        let quota = context.state.reference_library().quota().await?;
        let _ = cancel;
        Ok(Self {
            state: Arc::new(Mutex::new(IngressState {
                entries: Vec::new(),
                used_bytes: quota.used_bytes,
                used_entries: quota.object_count,
                max_bytes: quota.max_bytes,
                max_entries: quota.max_entries,
            })),
        })
    }

    fn stage(&self, title: String, media_type: String, data: Vec<u8>) -> Result<String, String> {
        let trimmed = title.trim();
        if trimmed.is_empty() || trimmed.chars().count() > 256 || trimmed.contains('\0') {
            return Err("title must be 1..=256 non-NUL characters".into());
        }
        if data.len() > MAX_IMAGE_BYTES {
            return Err("reference payload exceeds maximum object size".into());
        }
        let (canonical_mime, _, _) =
            inspect_binary_for_storage(&media_type, &data).map_err(|e| e.to_string())?;
        if canonical_mime.starts_with("text/") && std::str::from_utf8(&data).is_err() {
            return Err("text reference payload must be valid UTF-8".into());
        }
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if guard.entries.len() >= MAX_STAGED_OBJECTS_PER_CALL {
            return Err("exceeded per-invocation references limit".into());
        }
        let next_entries = guard.used_entries.saturating_add(1);
        if next_entries > guard.max_entries {
            return Err(
                "Personal Content Library entry quota exceeded; guide the user to Dashboard -> 个人内容库 to clean up entries"
                    .into(),
            );
        }
        let next_bytes = guard.used_bytes.saturating_add(data.len() as u64);
        if next_bytes > guard.max_bytes {
            return Err(
                "Personal Content Library storage quota exceeded (256 MiB default); guide the user to Dashboard -> 个人内容库 to clean up entries"
                    .into(),
            );
        }
        let id = format!("ref_{}", &uuid::Uuid::new_v4().simple().to_string()[..24]);
        guard.used_entries = next_entries;
        guard.used_bytes = next_bytes;
        guard.entries.push(StagedEntry {
            id: id.clone(),
            title: trimmed.to_owned(),
            media_type: canonical_mime,
            data,
        });
        Ok(id)
    }

    pub(super) fn take(&mut self) -> Option<StagedReferences> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if guard.entries.is_empty() {
            None
        } else {
            Some(StagedReferences {
                entries: std::mem::take(&mut guard.entries),
            })
        }
    }
}

impl StagedReferences {
    pub(super) async fn commit(
        self,
        context: &ToolContext,
        cancel: &CancellationToken,
    ) -> Result<Vec<LibraryObjectSummary>, Error> {
        let library = context.state.reference_library();
        let execution_id = context.execution_id.map(|id| id.0.to_string());
        let mut committed = Vec::with_capacity(self.entries.len());
        for entry in self.entries {
            let summary = library
                .add(
                    CreateLibraryObject::LocalBlob {
                        id: Some(entry.id),
                        title: entry.title,
                        media_type: entry.media_type,
                        data: entry.data,
                        origin: LocalBlobOrigin::ToolCapture,
                        origin_execution_id: execution_id.clone(),
                    },
                    cancel.clone(),
                )
                .await?;
            committed.push(summary);
        }
        let _ = context
            .state
            .sync_library_projection(CancellationToken::new())
            .await;
        let _ = LimitKind::LibraryStorageBytes;
        Ok(committed)
    }
}

pub(super) fn valid_import(item: &ComponentItem, engine: &Engine) -> bool {
    let ComponentItem::ComponentInstance(interface) = item else {
        return false;
    };
    let mut exports = interface.exports(engine);
    let Some((name, func)) = exports.next() else {
        return false;
    };
    if name != "create" || exports.next().is_some() {
        return false;
    }
    let ComponentItem::ComponentFunc(func) = func.ty else {
        return false;
    };
    let mut dummy = Linker::<RuntimeState>::new(engine);
    if link(&mut dummy).is_err() {
        return false;
    }
    let params: Vec<_> = func.params().map(|(_, ty)| ty).collect();
    let matches_params = params.len() == 3
        && params[0] == wasmtime::component::Type::String
        && params[1] == wasmtime::component::Type::String
        && matches!(&params[2], wasmtime::component::Type::List(list_ty) if list_ty.ty() == wasmtime::component::Type::U8);
    matches_params
        && matches!(func.results().collect::<Vec<_>>().as_slice(), [wasmtime::component::Type::Result(r)]
            if r.ok() == Some(wasmtime::component::Type::String) && r.err() == Some(wasmtime::component::Type::String))
}

pub(super) fn link(linker: &mut Linker<RuntimeState>) -> wasmtime::Result<()> {
    let mut instance = linker.instance(INTERFACE)?;
    instance.func_wrap(
        "create",
        |store: StoreContextMut<'_, RuntimeState>,
         (title, media_type, data): (String, String, Vec<u8>)| {
            let Some(ingress) = store.data().references.clone() else {
                return Ok((Err(
                    "references capability is not bound for this invocation".into(),
                ),));
            };
            Ok((ingress.stage(title, media_type, data),))
        },
    )?;
    Ok(())
}
