use super::{destination::PinnedAddresses, *};
use tokio::net::TcpListener;

#[test]
fn bounded_normalized_source_has_safe_debug_and_destination() {
    let source =
        PublicHttpsUrl::parse("HTTPS://EXAMPLE.com:444/private?token=hidden%2Fquery").unwrap();
    assert_eq!(source.destination(), "https://example.com:444");
    assert_eq!(source.as_url().query(), Some("token=hidden%2Fquery"));
    let debug = format!("{source:?}");
    assert!(!debug.contains("private") && !debug.contains("hidden"));
    let full_query = format!("https://example.com/?{}", "x".repeat(MAX_QUERY_BYTES));
    assert!(PublicHttpsUrl::parse(&full_query).is_ok());
    assert_eq!(
        PublicHttpsUrl::parse(&(full_query + "x")),
        Err(PublicHttpsError::UrlLimit)
    );
    let full_path = format!("https://example.com/{}", "x".repeat(MAX_URL_BYTES - 20));
    assert!(PublicHttpsUrl::parse(&full_path).is_ok());
    assert_eq!(
        PublicHttpsUrl::parse(&(full_path + "x")),
        Err(PublicHttpsError::UrlLimit)
    );
}

#[test]
fn reject_ambiguous_or_nonpublic_source_literals() {
    for value in [
        "https:/example.com/image",
        "https:////example.com/image",
        "http://example.com/image",
        "https://@example.com/image",
        "https://user:secret@example.com/image",
        "https://example.com/#secret",
        "https://example.com/#",
        "https://example.com:0/image",
        "https://example.com/a b",
        "https://example.com/%qq",
        "https://example.com/?x=%",
        "https://example.com/a\\b",
        "https://example.com/a\tb",
        "https://example..com/",
    ] {
        assert_eq!(
            PublicHttpsUrl::parse(value),
            Err(PublicHttpsError::InvalidUrl),
            "{value}"
        );
    }
    for value in [
        "https://localhost/",
        "https://localhost./",
        "https://host.local/",
        "https://metadata.google.internal/path",
        "https://169.254.169.254/",
        "https://0177.1/",
        "https://2130706433/",
        "https://0x7f000001/",
        "https://[::ffff:a9fe:a9fe]/",
        "https://[::ffff:7f00:1]/",
        "https://[64:ff9b::a9fe:a9fe]/",
        "https://[2002:7f00:0001::]/",
    ] {
        assert_eq!(
            PublicHttpsUrl::parse(value),
            Err(PublicHttpsError::NonPublic),
            "{value}"
        );
    }
    for value in [
        "https://example.com./",
        "https://example.media/",
        "https://例え.jp/",
        "https://1.1.1.1:10443/",
        "https://[::ffff:1.1.1.1]/",
        "https://[2606:4700:4700::1111]/",
    ] {
        assert!(PublicHttpsUrl::parse(value).is_ok(), "{value}");
    }
}

#[test]
fn entire_dns_set_and_actual_peer_must_be_public_and_pinned() {
    let socket = |value: &str| value.parse::<SocketAddr>().unwrap();
    let public = socket("1.1.1.1:443");
    let alternate = socket("8.8.8.8:443");
    for private in [
        "0.0.0.0:443",
        "10.23.4.5:443",
        "100.64.0.1:443",
        "127.0.0.1:443",
        "169.254.169.254:443",
        "172.16.0.1:443",
        "192.168.0.1:443",
        "192.0.2.4:443",
        "198.18.1.2:443",
        "198.51.100.42:443",
        "203.0.113.42:443",
        "239.1.1.1:443",
        "[::]:443",
        "[::1]:443",
        "[::ffff:127.0.0.1]:443",
        "[::ffff:10.1.2.3]:443",
        "[fc00::1]:443",
        "[fe80::1%1]:443",
        "[ff02::1]:443",
        "[2001:db8::1]:443",
        "[2001::1]:443",
        "[3fff::1]:443",
        "[64:ff9b:1::1]:443",
    ] {
        let private = socket(private);
        assert_eq!(
            PinnedAddresses::from_answers([public, private], 443).unwrap_err(),
            PublicHttpsError::NonPublic,
            "{private}"
        );
        assert_eq!(
            PinnedAddresses::from_answers([private, public], 443).unwrap_err(),
            PublicHttpsError::NonPublic,
            "{private}"
        );
    }
    let pin = PinnedAddresses::from_answers([public, alternate], 443).unwrap();
    let mapped = socket("[::ffff:1.1.1.1]:443");
    let same = PinnedAddresses::from_answers([alternate, mapped, public], 443).unwrap();
    assert_eq!(pin.check_unchanged(&same), Ok(()));
    let changed = PinnedAddresses::from_answers([socket("8.8.4.4:443"), public], 443).unwrap();
    assert_eq!(
        pin.check_unchanged(&changed),
        Err(PublicHttpsError::DestinationChanged)
    );
    assert_eq!(
        PinnedAddresses::from_answers([], 443).unwrap_err(),
        PublicHttpsError::Resolution
    );
    assert_eq!(
        PinnedAddresses::from_answers([public; 65], 443).unwrap_err(),
        PublicHttpsError::AddressLimit
    );
    assert!(pin.contains(public) && pin.contains(alternate) && pin.contains(mapped));
    assert!(!pin.contains(socket("1.1.1.1:80")));
    assert!(!pin.contains(socket("8.8.4.4:443")));
    assert!(!pin.contains(socket("[::ffff:127.0.0.1]:443")));
    assert!(!format!("{pin:?}").contains("1.1.1.1"));
}

#[tokio::test]
async fn real_loopback_listener_is_refused_before_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    for host in [
        addr.ip().to_string(),
        "[::ffff:127.0.0.1]".to_owned(),
        "localhost".to_owned(),
    ] {
        let source = format!("https://{host}:{}/image?secret=never_sent", addr.port());
        assert_eq!(
            PublicHttpsUrl::parse(&source),
            Err(PublicHttpsError::NonPublic)
        );
    }
    assert!(
        timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancel_has_priority_and_deadline_drops_inflight_work() {
    let url = PublicHttpsUrl::parse("https://1.1.1.1/image?secret=hidden").unwrap();
    let cancel = CancellationToken::new();
    let pin = url.admit(&cancel).await.unwrap(); // literal; no DNS or GET
    cancel.cancel();
    assert_eq!(
        url.admit(&cancel).await.unwrap_err(),
        PublicHttpsError::Cancelled
    );
    assert!(matches!(
        pin.fetch(64, &cancel).await,
        Err(PublicHttpsError::Cancelled)
    ));
    assert!(!format!("{:?}", PublicHttpsError::Connect).contains("hidden"));

    let cancel = CancellationToken::new();
    let result = bounded::<()>(
        &cancel,
        Duration::from_millis(5),
        PublicHttpsError::Timeout,
        std::future::pending(),
    )
    .await;
    assert_eq!(result, Err(PublicHttpsError::Timeout));
    let work = bounded::<()>(
        &cancel,
        Duration::from_secs(1),
        PublicHttpsError::Timeout,
        std::future::pending(),
    );
    let (result, ()) = tokio::join!(work, async {
        tokio::task::yield_now().await;
        cancel.cancel();
    });
    assert_eq!(result, Err(PublicHttpsError::Cancelled));
}

#[cfg(not(windows))]
mod tls {
    use super::*;
    use rustls::{
        ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    };
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const CERTIFICATE: &[u8] = include_bytes!("../http/fixtures/localhost-cert.der");
    const PRIVATE_KEY: &[u8] = include_bytes!("../http/fixtures/localhost-key.der");
    const FIXTURE_LIMIT: Duration = Duration::from_secs(3);

    async fn fixture(
        response: &str,
        max_bytes: usize,
        peer_matches: bool,
    ) -> Result<PublicHttpsImage, PublicHttpsError> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![CertificateDer::from(CERTIFICATE.to_vec())],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(PRIVATE_KEY.to_vec())),
                )
                .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let client = guarded_client()
            .resolve("localhost", address)
            .tls_built_in_root_certs(false)
            .add_root_certificate(reqwest::Certificate::from_der(CERTIFICATE).unwrap())
            .build()
            .unwrap();
        let url = Url::parse(&format!(
            "https://localhost:{}/some/image?private=fixture",
            address.port()
        ))
        .unwrap();
        // Test only the wire layer with a loopback fixture and isolated trust root;
        // the production pin will never allow this peer.
        let (result, ()) = timeout(FIXTURE_LIMIT, async {
            tokio::join!(
                transfer(&client, url, max_bytes, |peer| peer_matches
                    && peer == address),
                async {
                    let (socket, _) = listener.accept().await.unwrap();
                    let mut tls = acceptor.accept(socket).await.unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        request.push(tls.read_u8().await.unwrap());
                        assert!(request.len() < 4096);
                    }
                    let request = std::str::from_utf8(&request).unwrap();
                    assert!(request.starts_with("GET /some/image?private=fixture HTTP/1.1\r\n"));
                    assert!(request.contains("accept-encoding: identity\r\n"));
                    assert!(!request.contains("cookie:") && !request.contains("authorization:"));
                    tls.write_all(response.as_bytes()).await.unwrap();
                    tls.flush().await.unwrap();
                }
            )
        })
        .await
        .unwrap();
        assert!(
            timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err(),
            "one fetch must not open a second connection"
        );
        result
    }

    #[tokio::test]
    async fn one_get_retains_exact_body_and_does_not_follow_redirect() {
        let response = "HTTP/1.1 200 OK\r\nContent-Type: Image/PNG; anything=ignored\r\nContent-Length: 4\r\nConnection: close\r\n\r\nABCD";
        let image = fixture(response, 4, true).await.unwrap();
        assert_eq!(image.mime_type, "image/png");
        assert_eq!(image.bytes, b"ABCD");
        assert!(matches!(
            fixture(response, 4, false).await,
            Err(PublicHttpsError::PeerMismatch)
        ));

        // Relative redirects would hit this same fixture a second time if enabled.
        let redirect = "HTTP/1.1 302 Found\r\nLocation: /redirected?private=sensitive\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        assert!(matches!(
            fixture(redirect, 4, true).await,
            Err(PublicHttpsError::Redirect)
        ));
    }

    #[tokio::test]
    async fn declared_and_chunked_sizes_and_encodings_are_bounded() {
        let long = "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: 5\r\nConnection: close\r\n\r\nABCDE";
        assert!(matches!(
            fixture(long, 4, true).await,
            Err(PublicHttpsError::BodyLimit)
        ));
        let chunked = "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n2\r\nAB\r\n3\r\nCDE\r\n0\r\n\r\n";
        assert!(matches!(
            fixture(chunked, 4, true).await,
            Err(PublicHttpsError::BodyLimit)
        ));
        let encoded = "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Encoding: gzip\r\nContent-Length: 4\r\nConnection: close\r\n\r\nABCD";
        assert!(matches!(
            fixture(encoded, 4, true).await,
            Err(PublicHttpsError::ContentEncoding)
        ));
        let transfer_coded = "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nTransfer-Encoding: gzip, chunked\r\nConnection: close\r\n\r\n4\r\nABCD\r\n0\r\n\r\n";
        assert!(matches!(
            fixture(transfer_coded, 4, true).await,
            Err(PublicHttpsError::TransferEncoding)
        ));
        let not_image = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 4\r\nConnection: close\r\n\r\nABCD";
        assert!(matches!(
            fixture(not_image, 4, true).await,
            Err(PublicHttpsError::ContentType)
        ));
        let duplicate = "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Type: image/jpeg\r\nContent-Length: 4\r\nConnection: close\r\n\r\nABCD";
        assert!(matches!(
            fixture(duplicate, 4, true).await,
            Err(PublicHttpsError::ContentType)
        ));
    }
}
