use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Declared {
    name: &'static str,
    bound: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for Declared {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name.into(),
            description: "fixture".into(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    fn media_output_bytes(&self) -> usize {
        self.bound.load(Ordering::SeqCst)
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        Ok(ToolOutput::text("done"))
    }
}
#[test]
fn media_declarations_are_snapshotted_from_the_registered_implementation() {
    let bound = Arc::new(AtomicUsize::new(64));
    let registry = ToolRegistry::new(vec![
        ToolRegistration::External(Arc::new(Declared {
            name: "arbitrary",
            bound: bound.clone(),
        })),
        ToolRegistration::External(Arc::new(Declared {
            name: "read_image",
            bound: Arc::new(AtomicUsize::new(0)),
        })),
    ])
    .unwrap();
    bound.store(0, Ordering::SeqCst);
    assert_eq!(registry.media_output_bytes("arbitrary"), 64);
    assert_eq!(registry.media_output_bytes("read_image"), 0);
    assert_eq!(registry.media_output_bytes("unknown"), 0);
}
#[test]
fn registered_source_budget_cannot_exceed_image_admission_safety_ceiling() {
    assert!(
        ToolRegistry::new(vec![ToolRegistration::External(Arc::new(Declared {
            name: "image",
            bound: Arc::new(AtomicUsize::new(
                crate::admission::image::MAX_IMAGE_BYTES + 1
            ))
        }))])
        .is_err()
    );
}
