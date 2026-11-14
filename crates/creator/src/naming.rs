//! Creator-owned naming schedule; runtime owns the clean rename model call/prompt.
use async_trait::async_trait;
use microcrab_protocol::{InputPart, SessionInput};
use microcrab_session::{Error, Session};

/// A backend-specific owner of naming metadata/operation receipts. claim must
/// durably mark the attempt before returning true; false means already attempted
/// or disabled. A failed claim never permits a model call. Title publication is
/// independent of Journal HEAD, model history and reversible SessionState.
#[async_trait]
pub trait AutoNamingStore: Send + Sync {
    async fn claim(&self) -> Result<bool, Error>;
    async fn set_title(&self, title: &str) -> Result<(), Error>;
}

/// Invoke for the first durably admitted user text, independently of the primary
/// turn outcome. An attempted rename is never automatically reissued after provider
/// error, cancellation or process restart. Image-only input must not claim naming.
pub async fn maybe_rename(
    session: &Session,
    input: &SessionInput,
    store: &dyn AutoNamingStore,
) -> Result<Option<String>, Error> {
    let text = input
        .parts
        .iter()
        .filter_map(|part| match part {
            InputPart::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    if text.trim().is_empty() || !store.claim().await? {
        return Ok(None);
    }
    // The built-in function owns prompt and response validation. Do not create a
    // second server naming prompt or permit naming to call arbitrary tools.
    let response = session.rename_with_model(text).await?;
    store.set_title(&response.content).await?;
    Ok(Some(response.content))
}

#[cfg(test)]
mod tests {
    use super::*;
    use microcrab_auditor::{Auditor, VariableView};
    use microcrab_protocol::{Response, Usage};
    use microcrab_session::{Client, runtime::Runtime};
    use microcrab_transport::Content;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tokio_util::sync::CancellationToken;

    struct Model(Arc<AtomicUsize>);
    #[async_trait]
    impl Client for Model {
        async fn complete(
            &self,
            _: &Content,
            _: &dyn Auditor,
            _: &dyn VariableView,
            _: CancellationToken,
        ) -> Result<Response, microcrab_transport::Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Response {
                provider_metadata: None,
                content: "Native title".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            })
        }
    }

    struct Store {
        attempted: AtomicBool,
        titles: Mutex<Vec<String>>,
        fail: bool,
    }
    #[async_trait]
    impl AutoNamingStore for Store {
        async fn claim(&self) -> Result<bool, Error> {
            if self.fail {
                return Err(Error::Audit("claim persistence failed".into()));
            }
            Ok(!self.attempted.swap(true, Ordering::SeqCst))
        }
        async fn set_title(&self, title: &str) -> Result<(), Error> {
            self.titles.lock().unwrap().push(title.to_owned());
            Ok(())
        }
    }

    #[tokio::test]
    async fn naming_requires_persisted_claim_and_never_repeats() {
        let calls = Arc::new(AtomicUsize::new(0));
        let runtime = Runtime::new(vec![], Arc::new(crate::DeclarativePermission)).unwrap();
        let session = crate::assemble(Session::builder(Model(calls.clone()), runtime), None, None)
            .await
            .unwrap();
        let input = SessionInput::text("review project");
        let failed = Store {
            attempted: AtomicBool::new(false),
            titles: Mutex::default(),
            fail: true,
        };
        assert!(maybe_rename(&session, &input, &failed).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let store = Store {
            attempted: AtomicBool::new(false),
            titles: Mutex::default(),
            fail: false,
        };
        let image_only = SessionInput {
            parts: vec![InputPart::Image {
                source: microcrab_protocol::ImageSource::Path {
                    path: "image.png".into(),
                },
            }],
            variables: Default::default(),
        };
        assert_eq!(
            maybe_rename(&session, &image_only, &store).await.unwrap(),
            None
        );
        assert_eq!(
            maybe_rename(&session, &SessionInput::text("  "), &store)
                .await
                .unwrap(),
            None
        );
        assert!(!store.attempted.load(Ordering::SeqCst));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            maybe_rename(&session, &input, &store).await.unwrap(),
            Some("Native title".into())
        );
        assert_eq!(maybe_rename(&session, &input, &store).await.unwrap(), None);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.titles.lock().unwrap().as_slice(), &["Native title"]);
        session.close().await.unwrap();
    }
}
