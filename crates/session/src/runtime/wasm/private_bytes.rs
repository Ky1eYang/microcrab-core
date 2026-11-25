//! Write-only SessionState ingress from a normally admitted guest tool.
use super::{RuntimeState, ToolContext};
use crate::{Error, admission::image, state::access::RedirectDestination};
use tokio_util::sync::CancellationToken;
use wasmtime::{
    Engine, StoreContextMut,
    component::{Linker, types::ComponentItem},
};

pub(super) const INTERFACE: &str = "microcrab:session/private-bytes@0.2.1";

pub(super) fn valid_import(item: &ComponentItem, engine: &Engine) -> bool {
    matches!(item, ComponentItem::ComponentInstance(interface) if {
        let names = interface.exports(engine).map(|(name, export)| (name, export.ty)).collect::<Vec<_>>();
        names.len() == 1 && names[0].0 == "create" && matches!(names[0].1, ComponentItem::ComponentFunc(_))
    })
}

pub(super) struct Staged {
    destination: RedirectDestination,
    media_type: String,
    data: Vec<u8>,
}

pub(super) struct Ingress {
    destination: RedirectDestination,
    staged: Option<Staged>,
    attempted: bool,
}

impl Ingress {
    pub(super) fn new(context: &ToolContext, name: &str) -> Result<Self, Error> {
        Ok(Self {
            destination: context.state.prepare_redirect_destination(name)?,
            staged: None,
            attempted: false,
        })
    }

    pub(super) fn take(&mut self) -> Option<Staged> {
        self.staged.take()
    }
}

impl Staged {
    pub(super) async fn commit(
        self,
        context: &ToolContext,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        context
            .state
            .commit_private_image(self.destination, &self.media_type, self.data, cancel)
            .await?;
        Ok(())
    }
}

fn stage(
    state: &mut RuntimeState,
    name: String,
    media_type: String,
    data: Vec<u8>,
) -> Result<(), &'static str> {
    let unavailable = "no admitted private Bytes destination";
    let context = state.tool_context.as_ref().ok_or(unavailable)?;
    context.ensure_admitted(None).map_err(|_| unavailable)?;
    let ingress = state.private_bytes.as_mut().ok_or(unavailable)?;
    if std::mem::replace(&mut ingress.attempted, true) {
        ingress.staged = None;
        return Err("private Bytes may be created only once per invocation");
    }
    if ingress.destination.name() != name
        || state.binding.is_none()
        || state.cancellation.is_cancelled()
        || state.epoch_cancellation.is_cancelled()
        || std::time::Instant::now() >= state.deadline
    {
        return Err(unavailable);
    }
    let max_bytes = context
        .state
        .snapshot()
        .limits
        .max_tool_image_bytes
        .min(image::MAX_IMAGE_BYTES)
        .min(state.output_bytes_remaining);
    if data.is_empty() || data.len() > max_bytes {
        return Err("private Bytes image exceeds the admitted byte limit");
    }
    image::validate_image(&media_type, &data)
        .map_err(|_| "private Bytes requires a supported raster image")?;
    state.output_bytes_remaining -= data.len();
    ingress.staged = Some(Staged {
        destination: ingress.destination.clone(),
        media_type,
        data,
    });
    Ok(())
}

pub(super) fn link(linker: &mut Linker<RuntimeState>) -> wasmtime::Result<()> {
    linker.instance(INTERFACE)?.func_wrap(
        "create",
        |mut caller: StoreContextMut<'_, RuntimeState>,
         (name, mime, data): (String, String, Vec<u8>)| {
            Ok((stage(caller.data_mut(), name, mime, data).map_err(String::from),))
        },
    )?;
    Ok(())
}
