//! Real loopback HTTPS exercises the production platform TLS backend.
//! The fixture private key is deliberately public, throwaway test material. Never
//! use it outside these tests. No process-wide Rustls provider is installed.
use std::{error::Error, net::SocketAddr, time::Duration};

#[cfg(not(windows))]
use std::sync::Arc;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};

const CERTIFICATE: &[u8] = include_bytes!("fixtures/localhost-cert.der");
const PRIVATE_KEY: &[u8] = include_bytes!("fixtures/localhost-key.der");
const BODY: &str = "platform HTTPS regression\n";
const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
enum TlsVersion {
    Tls12,
    Tls13,
}

#[cfg(windows)]
fn supports_tls13() -> bool {
    use windows_sys::{
        Wdk::System::SystemServices::RtlGetVersion,
        Win32::System::SystemInformation::{OSVERSIONINFOEXW, OSVERSIONINFOW},
    };

    let mut version = OSVERSIONINFOEXW {
        dwOSVersionInfoSize: std::mem::size_of::<OSVERSIONINFOEXW>() as u32,
        ..Default::default()
    };
    // SAFETY: RtlGetVersion accepts OSVERSIONINFOEXW when its size field is set
    // accordingly. This writable, aligned structure outlives the synchronous
    // call; casting its address to the API's base pointer preserves its layout.
    let status =
        unsafe { RtlGetVersion((&mut version as *mut OSVERSIONINFOEXW).cast::<OSVERSIONINFOW>()) };
    assert_eq!(
        status, 0,
        "query real Windows version: NTSTATUS {status:#X}"
    );
    assert!(
        matches!(version.wProductType, 1..=3),
        "unexpected Windows product type {}",
        version.wProductType
    );
    // Microsoft documents TLS 1.3 from Windows 11 / Server 2022. Product type
    // VER_NT_WORKSTATION (1) distinguishes the client build floor from servers.
    // https://learn.microsoft.com/windows/win32/secauthn/protocols-in-tls-ssl--schannel-ssp-
    let minimum_build = if version.wProductType == 1 {
        22000
    } else {
        20348
    };
    let supported = version.dwMajorVersion > 10
        || (version.dwMajorVersion == 10 && version.dwBuildNumber >= minimum_build);
    if !supported {
        eprintln!(
            "TLS 1.3 NOT EXERCISED: Schannel does not support it on Windows {}.{} build {} (product type {}); TLS 1.2 regressions remain mandatory",
            version.dwMajorVersion,
            version.dwMinorVersion,
            version.dwBuildNumber,
            version.wProductType
        );
    }
    supported
}

#[cfg(not(windows))]
fn supports_tls13() -> bool {
    true
}

fn supported_versions() -> &'static [TlsVersion] {
    if supports_tls13() {
        &[TlsVersion::Tls12, TlsVersion::Tls13]
    } else {
        &[TlsVersion::Tls12]
    }
}

#[cfg(windows)]
type TestAcceptor = tokio_native_tls::TlsAcceptor;
#[cfg(not(windows))]
type TestAcceptor = tokio_rustls::TlsAcceptor;
#[cfg(windows)]
type TestStream = tokio_native_tls::TlsStream<TcpStream>;
#[cfg(not(windows))]
type TestStream = tokio_rustls::server::TlsStream<TcpStream>;

#[cfg(windows)]
fn acceptor(version: TlsVersion) -> TestAcceptor {
    use base64::{Engine, engine::general_purpose::STANDARD};

    // Schannel imports PKCS#8 PEM directly; keep fixture conversion in memory,
    // without an external certificate generator or a temporary file.
    fn pem(label: &str, der: &[u8]) -> Vec<u8> {
        format!(
            "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
            STANDARD.encode(der)
        )
        .into_bytes()
    }
    let identity = native_tls::Identity::from_pkcs8(
        &pem("CERTIFICATE", CERTIFICATE),
        &pem("PRIVATE KEY", PRIVATE_KEY),
    )
    .expect("import the public test identity into Schannel");
    let protocol = match version {
        TlsVersion::Tls12 => native_tls::Protocol::Tlsv12,
        TlsVersion::Tls13 => native_tls::Protocol::Tlsv13,
    };
    let mut builder = native_tls::TlsAcceptor::builder(identity);
    // Pin both ends of the interval: a successful handshake proves the selected
    // version, including TLS 1.3 on Windows versions that support it.
    builder.min_protocol_version(Some(protocol));
    builder.max_protocol_version(Some(protocol));
    builder.build().expect("build Schannel test server").into()
}

#[cfg(not(windows))]
fn acceptor(version: TlsVersion) -> TestAcceptor {
    use rustls::{
        ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    };

    let protocol = match version {
        TlsVersion::Tls12 => &rustls::version::TLS12,
        TlsVersion::Tls13 => &rustls::version::TLS13,
    };
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(PRIVATE_KEY.to_vec()));
    // An explicit test-server provider avoids installing any global provider.
    let config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[protocol])
            .expect("test provider supports the selected TLS version")
            .with_no_client_auth()
            .with_single_cert(vec![CertificateDer::from(CERTIFICATE.to_vec())], key)
            .expect("fixture certificate matches its throwaway private key");
    TestAcceptor::from(Arc::new(config))
}

async fn accept(
    acceptor: TestAcceptor,
    socket: TcpStream,
    version: TlsVersion,
) -> Result<TestStream, String> {
    let stream = acceptor
        .accept(socket)
        .await
        .map_err(|error| error.to_string())?;
    #[cfg(not(windows))]
    {
        let expected = match version {
            TlsVersion::Tls12 => rustls::ProtocolVersion::TLSv1_2,
            TlsVersion::Tls13 => rustls::ProtocolVersion::TLSv1_3,
        };
        assert_eq!(stream.get_ref().1.protocol_version(), Some(expected));
    }
    #[cfg(windows)]
    let _ = version; // Schannel's server protocol interval is pinned above.
    Ok(stream)
}

#[derive(Debug)]
enum ServerOutcome {
    Served { request: String },
    Rejected(String),
}

struct HttpsServer {
    address: SocketAddr,
    task: Option<JoinHandle<Result<ServerOutcome, String>>>,
}

impl Drop for HttpsServer {
    fn drop(&mut self) {
        // A client assertion/panic must not leave an accept or handshake running.
        // Normal completion additionally joins the task in finish().
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl HttpsServer {
    async fn start(version: TlsVersion) -> Self {
        let acceptor = acceptor(version);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback HTTPS listener");
        let address = listener.local_addr().expect("listener address");
        let task = tokio::spawn(async move {
            timeout(DEADLINE, async move {
                let (socket, _) = listener.accept().await.map_err(|error| error.to_string())?;
                let mut stream = match accept(acceptor, socket, version).await {
                    Ok(stream) => stream,
                    Err(error) => return Ok(ServerOutcome::Rejected(error)),
                };
                let mut request = Vec::new();
                while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let mut buffer = [0; 1024];
                    let length = match stream.read(&mut buffer).await {
                        Ok(0) if request.is_empty() => {
                            return Ok(ServerOutcome::Rejected(
                                "client closed after TLS, before sending HTTP".to_owned(),
                            ));
                        }
                        Err(error) if request.is_empty() => {
                            // Schannel may complete the server handshake before
                            // the client reports its certificate-validation error.
                            return Ok(ServerOutcome::Rejected(error.to_string()));
                        }
                        result => result.map_err(|error| error.to_string())?,
                    };
                    if length == 0 {
                        return Err("client closed before completing HTTP headers".to_owned());
                    }
                    request.extend_from_slice(&buffer[..length]);
                    if request.len() > 8192 {
                        return Err("test request exceeded the header bound".to_owned());
                    }
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{BODY}",
                    BODY.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .map_err(|error| error.to_string())?;
                stream.flush().await.map_err(|error| error.to_string())?;
                Ok(ServerOutcome::Served {
                    request: String::from_utf8(request).map_err(|error| error.to_string())?,
                })
            })
            .await
            .map_err(|_| "loopback HTTPS server timed out".to_owned())?
        });
        Self {
            address,
            task: Some(task),
        }
    }

    fn url(&self, hostname: &str) -> String {
        format!("https://{hostname}:{}/regression", self.address.port())
    }

    async fn finish(mut self) -> ServerOutcome {
        let mut task = self.task.take().expect("server task is joined once");
        match timeout(DEADLINE, &mut task).await {
            Ok(result) => result
                .expect("loopback HTTPS task did not panic")
                .expect("loopback HTTPS server completed"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                panic!("loopback HTTPS task exceeded its join deadline");
            }
        }
    }
}

fn trusted_builder() -> reqwest::ClientBuilder {
    super::client_builder()
        .tls_built_in_root_certs(false)
        .add_root_certificate(
            reqwest::Certificate::from_der(CERTIFICATE)
                .expect("parse the self-signed localhost trust anchor"),
        )
}

fn client(builder: reqwest::ClientBuilder, server: &HttpsServer) -> reqwest::Client {
    builder
        .no_proxy()
        // Avoid external DNS and proxy settings on developer machines and CI.
        .resolve("localhost", server.address)
        .timeout(DEADLINE)
        .build()
        .expect("build client with the production platform TLS backend")
}

async fn assert_https(version: TlsVersion) {
    let server = HttpsServer::start(version).await;
    let result = timeout(DEADLINE, async {
        let response = client(trusted_builder(), &server)
            .get(server.url("localhost"))
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        Ok::<_, reqwest::Error>((status, body))
    })
    .await;
    let outcome = server.finish().await;
    let (status, body) = result
        .unwrap_or_else(|error| {
            panic!("{version:?} request deadline: {error}; server: {outcome:?}")
        })
        .unwrap_or_else(|error| {
            panic!(
                "{version:?} custom-root HTTPS failed: {}; server: {outcome:?}",
                error_chain(&error)
            )
        });
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body, BODY);
    match outcome {
        ServerOutcome::Served { request } => {
            assert!(
                request.starts_with("GET /regression HTTP/1.1\r\n"),
                "{request}"
            );
        }
        other => panic!("trusted HTTPS handshake failed: {other:?}"),
    }
}

#[tokio::test]
async fn custom_trust_root_serves_https_over_tls12() {
    assert_https(TlsVersion::Tls12).await;
}

#[tokio::test]
async fn custom_trust_root_serves_https_over_tls13() {
    if !supports_tls13() {
        return;
    }
    assert_https(TlsVersion::Tls13).await;
}

fn error_chain(error: &(dyn Error + 'static)) -> String {
    let mut messages = vec![error.to_string()];
    let mut source = error.source();
    while let Some(error) = source {
        messages.push(error.to_string());
        source = error.source();
    }
    messages.join(": ")
}

async fn assert_certificate_rejected(version: TlsVersion, trust_fixture: bool) {
    let server = HttpsServer::start(version).await;
    let builder = if trust_fixture {
        trusted_builder()
    } else {
        // Exercise the public production roots, which must never trust the
        // repository's self-signed fixture.
        super::client_builder()
    };
    let hostname = if trust_fixture {
        "127.0.0.1"
    } else {
        "localhost"
    };
    let result = timeout(
        DEADLINE,
        client(builder, &server).get(server.url(hostname)).send(),
    )
    .await;
    let outcome = server.finish().await;
    let error = result
        .expect("certificate rejection finished before its deadline")
        .expect_err("certificate validation must reject this connection");
    assert!(
        error.is_connect(),
        "expected TLS connection failure: {error}"
    );
    assert!(
        !error.is_timeout(),
        "TLS rejection must not be a timeout: {error}"
    );
    let chain = error_chain(&error);
    #[cfg(not(windows))]
    {
        let expected = if trust_fixture {
            "certificate not valid for name"
        } else {
            "UnknownIssuer"
        };
        assert!(chain.contains(expected), "expected {expected}, got {chain}");
    }
    #[cfg(windows)]
    {
        // Match Schannel's numeric validation statuses rather than localized OS
        // messages. Both SSPI and certificate-chain APIs can report the error.
        let expected = if trust_fixture {
            [0x80090322_u32, 0x800B010F_u32] // wrong principal / CN mismatch
        } else {
            [0x80090325_u32, 0x800B0109_u32] // untrusted root
        };
        let chain_upper = chain.to_ascii_uppercase();
        assert!(
            expected.iter().any(|status| {
                chain_upper.contains(&format!("{status:X}"))
                    || chain_upper.contains(&(*status as i32).to_string())
                    || chain_upper.contains(&status.to_string())
            }),
            "{version:?}: expected Schannel certificate-validation failure {expected:X?}, got {chain}; server: {outcome:?}"
        );
    }
    match outcome {
        ServerOutcome::Rejected(message) => {
            assert!(!message.is_empty(), "server observed the TLS failure");
        }
        other => panic!("invalid certificate reached HTTP handling: {other:?}"),
    }
}

#[tokio::test]
async fn production_roots_reject_untrusted_self_signed_certificate() {
    for &version in supported_versions() {
        assert_certificate_rejected(version, false).await;
    }
}

#[tokio::test]
async fn trusted_certificate_still_rejects_wrong_hostname() {
    // The certificate's only SAN is DNS:localhost, never IP:127.0.0.1.
    for &version in supported_versions() {
        assert_certificate_rejected(version, true).await;
    }
}
