use super::{DNS_TIMEOUT, PublicHttpsError, bounded};
use reqwest::{Url, dns};
use std::{
    fmt, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

const MAX_ADDRESSES: usize = 64;
static DNS_SLOTS: Semaphore = Semaphore::const_new(8);

pub(super) fn check_host(url: &Url) -> Result<(), PublicHttpsError> {
    if let Some(ip) = literal(url) {
        return if is_public(ip) {
            Ok(())
        } else {
            Err(PublicHttpsError::NonPublic)
        };
    }
    let host = url.host_str().ok_or(PublicHttpsError::InvalidUrl)?;
    let host = host.strip_suffix('.').unwrap_or(host);
    // No bare/search-suffix names or special-use local names. Ordinary domain
    // names do not need an origin allowlist; all resolved addresses are checked.
    if !host.contains('.')
        || ["localhost", "local", "internal", "home.arpa", "onion"]
            .iter()
            .any(|suffix| {
                host == *suffix || host.strip_suffix(suffix).is_some_and(|p| p.ends_with('.'))
            })
    {
        return Err(PublicHttpsError::NonPublic);
    }
    if host.len() > 253
        || !host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
    {
        return Err(PublicHttpsError::InvalidUrl);
    }
    Ok(())
}

fn literal(url: &Url) -> Option<IpAddr> {
    let host = url.host_str()?;
    // URL IPv6 hosts use brackets; SocketAddr and IP policy do not.
    host.strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host)
        .parse()
        .ok()
}

/// The system resolver cannot always be interrupted. Keep its concurrency permit
/// in the blocking worker until that worker actually returns, even if the caller
/// times out or is cancelled. No async task can start an unbounded resolver queue.
pub(super) async fn resolve(
    url: &Url,
    cancel: &CancellationToken,
) -> Result<PinnedAddresses, PublicHttpsError> {
    bounded(
        cancel,
        DNS_TIMEOUT,
        PublicHttpsError::ResolutionTimeout,
        async {
            check_host(url)?;
            let port = url.port().unwrap_or(443);
            if let Some(ip) = literal(url) {
                return PinnedAddresses::from_answers([SocketAddr::new(ip, port)], port);
            }
            let host = url.host_str().ok_or(PublicHttpsError::InvalidUrl)?;
            let absolute_host = format!("{}.", host.trim_end_matches('.'));
            let permit = DNS_SLOTS
                .try_acquire()
                .map_err(|_| PublicHttpsError::Busy)?;
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let answers = (absolute_host.as_str(), port)
                    .to_socket_addrs()
                    .map_err(|_| PublicHttpsError::Resolution)?;
                PinnedAddresses::from_answers(answers, port)
            })
            .await
            .map_err(|_| PublicHttpsError::Resolution)?
        },
    )
    .await
}

pub(super) struct PinnedAddresses(Vec<SocketAddr>);

impl fmt::Debug for PinnedAddresses {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PinnedAddresses(..)")
    }
}

impl PinnedAddresses {
    pub(super) fn from_answers(
        addresses: impl IntoIterator<Item = SocketAddr>,
        port: u16,
    ) -> Result<Self, PublicHttpsError> {
        let mut approved = Vec::new();
        for (index, raw) in addresses.into_iter().enumerate() {
            if index >= MAX_ADDRESSES {
                return Err(PublicHttpsError::AddressLimit);
            }
            if !public_socket(raw, port) {
                return Err(PublicHttpsError::NonPublic);
            }
            let address = canonical_socket(raw);
            if !approved.contains(&address) {
                approved.push(address);
            }
        }
        if approved.is_empty() {
            return Err(PublicHttpsError::Resolution);
        }
        Ok(Self(approved))
    }

    pub(super) fn check_unchanged(&self, fresh: &Self) -> Result<(), PublicHttpsError> {
        if self.0.len() != fresh.0.len() || !self.0.iter().all(|addr| fresh.0.contains(addr)) {
            return Err(PublicHttpsError::DestinationChanged);
        }
        Ok(())
    }

    pub(super) fn contains(&self, peer: SocketAddr) -> bool {
        public_socket(peer, peer.port()) && self.0.contains(&canonical_socket(peer))
    }

    pub(super) fn resolver_for(&self, url: &Url) -> PinnedResolver {
        PinnedResolver {
            hostname: url.host_str().unwrap_or_default().to_owned(),
            addresses: self.0.clone(),
        }
    }
}

// Unlike an override backed by reqwest's default resolver, a hostname mismatch
// here fails closed rather than silently running a third DNS lookup.
pub(super) struct PinnedResolver {
    hostname: String,
    addresses: Vec<SocketAddr>,
}

impl dns::Resolve for PinnedResolver {
    fn resolve(&self, name: dns::Name) -> dns::Resolving {
        let approved = name
            .as_str()
            .eq_ignore_ascii_case(&self.hostname)
            .then(|| self.addresses.clone());
        Box::pin(async move {
            match approved {
                Some(addresses) => Ok(Box::new(addresses.into_iter()) as dns::Addrs),
                None => Err(io::Error::other("hostname is not pinned").into()),
            }
        })
    }
}

fn canonical_socket(socket: SocketAddr) -> SocketAddr {
    SocketAddr::new(socket.ip().to_canonical(), socket.port())
}

fn public_socket(socket: SocketAddr, port: u16) -> bool {
    socket.port() == port
        && !matches!(socket, SocketAddr::V6(addr) if addr.scope_id() != 0)
        && is_public(socket.ip())
}

fn is_public(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(ip) => public_v4(ip),
        IpAddr::V6(ip) => public_v6(ip),
    }
}

fn public_v4(ip: Ipv4Addr) -> bool {
    // Reject non-globally-reachable and deprecated IANA special-purpose ranges.
    // Keep the globally reachable anycast exceptions in 192.0.0.0/24.
    !matches!(ip.octets(),
        [0 | 10 | 127 | 224..=255, ..]
        | [100, 64..=127, ..]
        | [169, 254, ..]
        | [172, 16..=31, ..]
        | [192, 0, 0, 0..=8 | 11..=255]
        | [192, 0, 2, _]
        | [192, 88, 99, _]
        | [192, 168, ..]
        | [198, 18..=19, ..]
        | [198, 51, 100, _]
        | [203, 0, 113, _]
    )
}

fn public_v6(ip: Ipv6Addr) -> bool {
    let [a, b, c, d, e, f, g, h] = ip.segments();
    // Ordinary global unicast only. This excludes ULA, link/site-local,
    // multicast, v4-compatible literals, and the well-known NAT64 ranges.
    // Exclude transition and documentation ranges inside global unicast too.
    if !matches!(a, 0x2000..=0x3fff)
        || a == 0x2002
        || (a == 0x2001 && b == 0x0db8)
        || (a == 0x3fff && b < 0x1000)
    {
        return false;
    }
    if a == 0x2001 && b <= 0x01ff {
        // IETF protocol assignments are not generally public destinations;
        // keep the explicit public anycast and service prefixes.
        return matches!(
            [b, c, d, e, f, g, h],
            [1, 0, 0, 0, 0, 0, 1..=3] | [3, ..] | [4, 0x112, ..]
        );
    }
    true
}
