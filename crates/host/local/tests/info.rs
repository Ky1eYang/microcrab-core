use microcrab_host_api::{Host, OsFamily};
#[cfg(unix)]
use microcrab_host_api::{OpContext, ProcessApi, ShellApi, ShellRequest};
use microcrab_host_local::{LocalHost, ShellLimits};

#[test]
fn local_info_reports_actual_host_and_frozen_interpreter() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let info = host.info().unwrap();
    assert_eq!(info.os, std::env::consts::OS);
    assert_eq!(info.arch, std::env::consts::ARCH);
    assert!(std::path::Path::new(&info.shell).is_absolute());
    assert!(std::path::Path::new(&info.shell).is_file());
    assert_eq!(
        info.os_family,
        if cfg!(windows) {
            OsFamily::Windows
        } else {
            OsFamily::Unix
        }
    );
    let (path_separator, newline) = if cfg!(windows) {
        ("\\", "\r\n")
    } else {
        ("/", "\n")
    };
    assert_eq!(info.path_separator, path_separator);
    assert_eq!(info.newline, newline);
    assert!(info.capabilities.is_empty());
    assert_eq!(info, host.clone().info().unwrap());
}

#[cfg(unix)]
#[tokio::test]
async fn unix_host_executes_bash_syntax() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    // Arrays and [[ ]] distinguish the required Bash interpreter from POSIX sh.
    let ctx = OpContext::default();
    let handle = ShellApi::launch(
        &host,
        &ctx,
        ShellRequest {
            command: "values=(first second); [[ ${values[1]} == second ]] && printf bash-ok".into(),
            cwd: None,
            output_bytes: 1024,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        ProcessApi::wait(&host, &ctx, handle).await.unwrap().code,
        Some(0)
    );
    assert_eq!(
        ShellApi::drain(&host, &ctx, handle, 1024)
            .await
            .unwrap()
            .stdout,
        "bash-ok"
    );
    ProcessApi::release(&host, &ctx, handle).await.unwrap();
}
