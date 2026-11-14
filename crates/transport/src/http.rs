//! HTTP clients use the platform's mature TLS backend without overriding a
//! process-wide Rustls provider. Normal certificate and hostname checks remain
//! enabled; callers own additional trust roots and other HTTP client settings.

#[cfg(test)]
mod tests;

/// Start an HTTP client with Schannel on Windows and Rustls elsewhere.
/// Callers retain ownership of redirects, timeouts, proxies and connection pools.
pub fn client_builder() -> reqwest::ClientBuilder {
    #[cfg(windows)]
    {
        reqwest::Client::builder().use_native_tls()
    }
    #[cfg(not(windows))]
    {
        reqwest::Client::builder().use_rustls_tls()
    }
}
