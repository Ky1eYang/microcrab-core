//! Caller-owned cancellation scope shared by Session and runtime operations.
//! Drop always requests cancellation, including early returns and unwinding. The
//! token must belong to the operation; clone a child token when the parent lives on.
use tokio_util::sync::CancellationToken;

pub(crate) struct CancelOnDrop(pub(crate) CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
