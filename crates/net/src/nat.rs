//! Bounded NAT-PMP port mapping with correlated gateway responses.

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use thiserror::Error;
use tokio::{net::UdpSocket, time::timeout};

const NAT_PMP_VERSION: u8 = 0;
const REQUEST_BYTES: usize = 12;
const RESPONSE_BYTES: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MappingProtocol {
    Udp,
    Tcp,
}

impl MappingProtocol {
    const fn opcode(self) -> u8 {
        match self {
            Self::Udp => 1,
            Self::Tcp => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortMapping {
    pub protocol: MappingProtocol,
    pub internal_port: u16,
    pub external_port: u16,
    pub lifetime: Duration,
    pub gateway_epoch: u32,
}

#[derive(Debug, Error)]
pub enum NatPmpError {
    #[error("NAT-PMP I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("NAT-PMP gateway timed out")]
    Timeout,
    #[error("NAT-PMP response is malformed or does not match the request")]
    InvalidResponse,
    #[error("NAT-PMP gateway rejected the request with result code {0}")]
    Gateway(u16),
    #[error("NAT-PMP mapping lifetime exceeds u32 seconds")]
    Lifetime,
}

#[derive(Clone, Copy, Debug)]
pub struct NatPmpClient {
    gateway: SocketAddr,
    timeout: Duration,
}

impl NatPmpClient {
    #[must_use]
    pub const fn new(gateway: SocketAddr, timeout: Duration) -> Self {
        Self { gateway, timeout }
    }

    pub async fn map_port(
        self,
        protocol: MappingProtocol,
        internal_port: u16,
        requested_external_port: u16,
        lifetime: Duration,
    ) -> Result<PortMapping, NatPmpError> {
        if internal_port == 0 || !self.gateway.is_ipv4() {
            return Err(NatPmpError::InvalidResponse);
        }
        let lifetime = u32::try_from(lifetime.as_secs()).map_err(|_| NatPmpError::Lifetime)?;
        let mut request = [0_u8; REQUEST_BYTES];
        request[0] = NAT_PMP_VERSION;
        request[1] = protocol.opcode();
        request[4..6].copy_from_slice(&internal_port.to_be_bytes());
        request[6..8].copy_from_slice(&requested_external_port.to_be_bytes());
        request[8..12].copy_from_slice(&lifetime.to_be_bytes());
        let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))).await?;
        socket.send_to(&request, self.gateway).await?;
        let mut response = [0_u8; RESPONSE_BYTES + 1];
        let (length, source) = timeout(self.timeout, socket.recv_from(&mut response))
            .await
            .map_err(|_| NatPmpError::Timeout)??;
        if length != RESPONSE_BYTES
            || source != self.gateway
            || response[0] != NAT_PMP_VERSION
            || response[1] != protocol.opcode() | 0x80
        {
            return Err(NatPmpError::InvalidResponse);
        }
        let result = u16::from_be_bytes([response[2], response[3]]);
        if result != 0 {
            return Err(NatPmpError::Gateway(result));
        }
        let response_internal = u16::from_be_bytes([response[8], response[9]]);
        let external_port = u16::from_be_bytes([response[10], response[11]]);
        if response_internal != internal_port || external_port == 0 {
            return Err(NatPmpError::InvalidResponse);
        }
        Ok(PortMapping {
            protocol,
            internal_port,
            external_port,
            lifetime: Duration::from_secs(u64::from(u32::from_be_bytes([
                response[12],
                response[13],
                response[14],
                response[15],
            ]))),
            gateway_epoch: u32::from_be_bytes([response[4], response[5], response[6], response[7]]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mapping_is_correlated_with_a_local_gateway()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let gateway = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = gateway.local_addr()?;
        let server = tokio::spawn(async move {
            let mut request = [0_u8; REQUEST_BYTES];
            let (length, client) = gateway.recv_from(&mut request).await?;
            if length != REQUEST_BYTES || request[1] != 2 {
                return Err::<(), std::io::Error>(std::io::Error::other("invalid request"));
            }
            let mut response = [0_u8; RESPONSE_BYTES];
            response[1] = 0x82;
            response[4..8].copy_from_slice(&17_u32.to_be_bytes());
            response[8..10].copy_from_slice(&request[4..6]);
            response[10..12].copy_from_slice(&45_000_u16.to_be_bytes());
            response[12..16].copy_from_slice(&3_600_u32.to_be_bytes());
            gateway.send_to(&response, client).await?;
            Ok(())
        });
        let mapping = NatPmpClient::new(address, Duration::from_secs(1))
            .map_port(
                MappingProtocol::Tcp,
                16_493,
                16_493,
                Duration::from_hours(1),
            )
            .await?;
        assert_eq!(mapping.external_port, 45_000);
        assert_eq!(mapping.gateway_epoch, 17);
        assert_eq!(mapping.lifetime, Duration::from_hours(1));
        server.await??;
        Ok(())
    }
}
