use std::{net::SocketAddr, sync::Arc};

use librqbit_utp::{UtpSocketUdp, UtpStream};
use thiserror::Error;

use crate::peer::{Handshake, PeerCodecLimits, PeerConnection, PeerSessionError};

#[derive(Clone, Debug)]
pub struct UtpEndpoint {
    socket: Arc<UtpSocketUdp>,
}

#[derive(Debug, Error)]
pub enum UtpError {
    #[error("uTP transport failed: {0}")]
    Transport(String),
    #[error(transparent)]
    Peer(#[from] PeerSessionError),
}

impl UtpEndpoint {
    pub async fn bind(address: SocketAddr) -> Result<Self, UtpError> {
        let socket = UtpSocketUdp::new_udp(address)
            .await
            .map_err(|error| UtpError::Transport(error.to_string()))?;
        Ok(Self { socket })
    }

    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.socket.bind_addr()
    }

    pub async fn connect_peer(
        &self,
        address: SocketAddr,
        handshake: Handshake,
        limits: PeerCodecLimits,
    ) -> Result<PeerConnection, UtpError> {
        let stream = self
            .socket
            .connect(address)
            .await
            .map_err(|error| UtpError::Transport(error.to_string()))?;
        Ok(PeerConnection::from_stream(stream, handshake, limits).await?)
    }

    pub async fn accept_peer(
        &self,
        handshake: Handshake,
        limits: PeerCodecLimits,
    ) -> Result<PeerConnection, UtpError> {
        let stream = self
            .socket
            .accept()
            .await
            .map_err(|error| UtpError::Transport(error.to_string()))?;
        Ok(PeerConnection::from_stream(stream, handshake, limits).await?)
    }

    /// Accept a raw uTP stream so an incoming peer handshake can be routed by
    /// its info hash before the local side replies.
    pub async fn accept_stream(&self) -> Result<UtpStream, UtpError> {
        self.socket
            .accept()
            .await
            .map_err(|error| UtpError::Transport(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use dendrite_core::Sha1Hash;

    use crate::peer::{PeerEvent, PeerId, PeerMessage};

    use super::*;

    fn handshake(peer: u8) -> Handshake {
        Handshake {
            reserved: [0; 8],
            info_hash: Sha1Hash::from_bytes([7; 20]),
            peer_id: PeerId::from_bytes([peer; 20]),
        }
    }

    #[tokio::test]
    async fn endpoints_negotiate_peer_wire_over_utp() -> Result<(), Box<dyn std::error::Error>> {
        let server = UtpEndpoint::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
        let client = UtpEndpoint::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
        let server_endpoint = server.clone();
        let accept = tokio::spawn(async move {
            server_endpoint
                .accept_peer(handshake(2), PeerCodecLimits::default())
                .await
        });
        let client_peer = client
            .connect_peer(
                server.local_addr(),
                handshake(1),
                PeerCodecLimits::default(),
            )
            .await?;
        let mut server_peer = accept.await??;
        client_peer.send(PeerMessage::Interested).await?;
        loop {
            match server_peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Interested)) => break,
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("uTP peer disconnected".into()),
                _ => {}
            }
        }
        client_peer.shutdown();
        server_peer.shutdown();
        Ok(())
    }
}
