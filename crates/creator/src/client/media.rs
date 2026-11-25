//! Explicit route-hosting calls intentionally keep provider attempt metadata only.
use microcrab_auditor::{AuditBody, Auditor, Moment};
use microcrab_transport::{Content, ProjectedMedia};

pub(super) fn attempt_content<'a>(
    canonical: &'a Content,
    projected: Option<&'a ProjectedMedia>,
) -> (&'a Content, bool) {
    match projected.and_then(ProjectedMedia::content_if_live) {
        Some(media) => (media, true),
        None => (canonical, false),
    }
}

/// Default intervention is a no-op: a body-inspecting or rewriting hook must
/// not receive either the signed URL or the inline bytes on these calls.
pub(super) struct MetadataAudit<'a>(pub(super) &'a dyn Auditor);
impl Auditor for MetadataAudit<'_> {
    fn is_enabled(&self) -> bool {
        self.0.is_enabled()
    }

    fn publish(&self, moment: Moment, mut body: AuditBody) {
        body.sent_request = None;
        body.response = None;
        body.response_bytes = None;
        body.payload = microcrab_auditor::HookPayload::None;
        if let Some(usage) = &mut body.usage {
            usage.provider_details.clear();
        }
        // This header is provider-owned text, not a closed metadata enum.
        body.response_content_type = None;
        self.0.publish(moment, body);
    }
}
