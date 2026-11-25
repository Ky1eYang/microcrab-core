//! Component capability profiles are keyed by exact versioned imports, never by
//! configuration labels or guest-reported Agent names.
use super::web_transport;
use crate::Error;
use wasmtime::{
    Engine,
    component::types::{Component, ComponentItem},
};

/// A Project/Host capability required by a component's imports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentCapability {
    WebTransport,
}

impl ComponentCapability {
    /// Check a capability against a currently authorized project Host route.
    /// This is only a preflight; invocation still enforces the exact Host binding
    /// and the admitted tool call.
    pub fn available_on_project(
        self,
        host: &dyn microcrab_host_api::Host,
        project: Option<&str>,
    ) -> bool {
        let Some(project) = project else {
            return false;
        };
        match self {
            Self::WebTransport => web_transport::host_binding(host)
                .is_some_and(|binding| binding.project_id == project),
        }
    }

    /// Discover requirements first, then check the whole import profile. The
    /// restrictions apply regardless of import order or Host support.
    pub(super) fn discover(component: &Component, engine: &Engine) -> Result<Vec<Self>, Error> {
        let mut required = Vec::new();
        let has_legacy_web = component
            .imports(engine)
            .any(|(name, _)| name == web_transport::INTERFACE);
        for (name, import) in component.imports(engine) {
            if name == web_transport::SCOPED_INTERFACE {
                if !has_legacy_web {
                    return Err(Error::wasm("scoped web transport requires the exact legacy companion import"));
                }
                web_transport::validate_scoped_interface(&import.ty, engine)?;
                continue;
            }
            let Some(capability) = Self::for_import(name) else {
                continue;
            };
            capability.validate_interface(&import.ty, engine)?;
            if required.contains(&capability) {
                return Err(Error::wasm("duplicate component capability import"));
            }
            if required.iter().any(|&other| {
                !capability.is_compatible_with(other) || !other.is_compatible_with(capability)
            }) {
                return Err(Error::wasm("incompatible component capability imports"));
            }
            required.push(capability);
        }
        for (name, import) in component.imports(engine) {
            if name == web_transport::SCOPED_INTERFACE || Self::for_import(name).is_some() {
                continue;
            }
            for &capability in &required {
                if !capability.allows_ordinary_import(name, &import.ty, engine) {
                    return Err(Error::wasm_with_private(
                        "component has an unexpected import for its required capability",
                        format!(
                            "{} capability does not allow import {name:?}",
                            capability.name()
                        ),
                    ));
                }
            }
        }
        Ok(required)
    }

    pub(super) fn for_import(interface: &str) -> Option<Self> {
        match interface {
            web_transport::INTERFACE => Some(Self::WebTransport),
            _ => None,
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::WebTransport => web_transport::CAPABILITY,
        }
    }

    fn is_compatible_with(self, _other: Self) -> bool {
        match self {
            // This is a restricted profile: it may not be combined with another
            // privileged capability without an explicit policy change.
            Self::WebTransport => false,
        }
    }

    pub(super) fn allows_ordinary_import(
        self,
        name: &str,
        item: &ComponentItem,
        engine: &Engine,
    ) -> bool {
        match self {
            Self::WebTransport => web_transport::allows_ordinary_import(name, item, engine),
        }
    }

    pub(super) fn validate_interface(
        self,
        item: &ComponentItem,
        engine: &Engine,
    ) -> Result<(), Error> {
        match self {
            Self::WebTransport => web_transport::validate_interface(item, engine),
        }
    }

}
