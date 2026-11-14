//! Network resources are backend-owned and bound to operation cancellation.
use crate::types::{HandleId, HostError, OpContext, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::net::{IpAddr, SocketAddr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SocketHandle(pub HandleId);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ListenerHandle(pub HandleId);

#[async_trait]
pub trait NetApi: Send + Sync {
    async fn resolve(&self, _ctx: &OpContext, _host: &str) -> Result<Vec<IpAddr>> {
        Err(HostError::unsupported_known("net.resolve"))
    }
    async fn connect_tcp(&self, _ctx: &OpContext, _address: SocketAddr) -> Result<SocketHandle> {
        Err(HostError::unsupported_known("net.connect_tcp"))
    }
    async fn listen_tcp(&self, _ctx: &OpContext, _address: SocketAddr) -> Result<ListenerHandle> {
        Err(HostError::unsupported_known("net.listen_tcp"))
    }
    async fn accept(
        &self,
        _ctx: &OpContext,
        _listener: ListenerHandle,
    ) -> Result<(SocketHandle, SocketAddr)> {
        Err(HostError::unsupported_known("net.accept"))
    }
    /// Empty bytes mean EOF. Implementations must enforce max without over-read.
    async fn read(&self, _ctx: &OpContext, _socket: SocketHandle, _max: usize) -> Result<Bytes> {
        Err(HostError::unsupported_known("net.read"))
    }
    /// Returns actual progress; callers must account for short writes.
    async fn write(&self, _ctx: &OpContext, _socket: SocketHandle, _data: Bytes) -> Result<usize> {
        Err(HostError::unsupported_known("net.write"))
    }
    async fn close_socket(&self, _ctx: &OpContext, _socket: SocketHandle) -> Result<()> {
        Err(HostError::unsupported_known("net.close_socket"))
    }
    async fn close_listener(&self, _ctx: &OpContext, _listener: ListenerHandle) -> Result<()> {
        Err(HostError::unsupported_known("net.close_listener"))
    }
}
