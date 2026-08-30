use std::{io, net::SocketAddr, time::Duration};

use bytes::BytesMut;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::{
    Handshake, PeerCodecError, PeerCodecLimits, PeerId, PeerMessage, decode_message, encode_message,
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const COMMAND_CAPACITY: usize = 256;
const EVENT_CAPACITY: usize = 1024;
const READ_CHUNK_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerEvent {
    Connected { peer_id: PeerId },
    Message(PeerMessage),
    Disconnected,
    Failed(String),
}

#[derive(Debug, Error)]
pub enum PeerSessionError {
    #[error("peer I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("peer handshake timed out")]
    HandshakeTimeout,
    #[error("peer responded with a different info hash")]
    InfoHashMismatch,
    #[error(transparent)]
    Codec(#[from] PeerCodecError),
    #[error("peer session is closed")]
    Closed,
    #[error("peer encryption failed: {0}")]
    Encryption(String),
}

pub struct PeerConnection {
    commands: mpsc::Sender<PeerCommand>,
    events: mpsc::Receiver<PeerEvent>,
    cancellation: CancellationToken,
    remote_reserved: [u8; 8],
    remote_peer_id: PeerId,
}

/// A cloneable, send-only view of a live peer session. This lets bounded
/// protocol services such as a hole-punch rendezvous relay address another
/// peer without sharing ownership of its event stream.
#[derive(Clone, Debug)]
pub struct PeerSender {
    commands: mpsc::Sender<PeerCommand>,
}

struct PeerCommand {
    message: PeerMessage,
    written: oneshot::Sender<()>,
}

impl std::fmt::Debug for PeerConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PeerConnection")
            .finish_non_exhaustive()
    }
}

impl PeerConnection {
    pub async fn connect(
        address: SocketAddr,
        handshake: Handshake,
        limits: PeerCodecLimits,
    ) -> Result<Self, PeerSessionError> {
        let stream = timeout(HANDSHAKE_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|_| PeerSessionError::HandshakeTimeout)??;
        stream.set_nodelay(true)?;
        Self::negotiate(stream, handshake, limits).await
    }

    pub async fn connect_encrypted(
        address: SocketAddr,
        handshake: Handshake,
        limits: PeerCodecLimits,
    ) -> Result<Self, PeerSessionError> {
        let stream = timeout(HANDSHAKE_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|_| PeerSessionError::HandshakeTimeout)??;
        stream.set_nodelay(true)?;
        let encrypted = timeout(
            HANDSHAKE_TIMEOUT,
            crate::mse::initiate(stream, handshake.info_hash),
        )
        .await
        .map_err(|_| PeerSessionError::HandshakeTimeout)?
        .map_err(|error| PeerSessionError::Encryption(error.to_string()))?;
        Self::negotiate(encrypted, handshake, limits).await
    }

    pub async fn accept(
        stream: TcpStream,
        handshake: Handshake,
        limits: PeerCodecLimits,
    ) -> Result<Self, PeerSessionError> {
        stream.set_nodelay(true)?;
        Self::negotiate(stream, handshake, limits).await
    }

    pub async fn from_stream<S>(
        stream: S,
        handshake: Handshake,
        limits: PeerCodecLimits,
    ) -> Result<Self, PeerSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::negotiate(stream, handshake, limits).await
    }

    /// Read an initiator's handshake before selecting a torrent. Incoming
    /// listeners need this split phase because the info hash determines the
    /// local handshake and payload to serve.
    pub async fn receive_handshake<S>(stream: &mut S) -> Result<Handshake, PeerSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut incoming = [0_u8; super::HANDSHAKE_BYTES];
        timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut incoming))
            .await
            .map_err(|_| PeerSessionError::HandshakeTimeout)??;
        Ok(Handshake::decode(&incoming)?)
    }

    /// Finish an incoming handshake that was previously read with
    /// [`Self::receive_handshake`].
    pub async fn accept_incoming<S>(
        mut stream: S,
        remote: Handshake,
        handshake: Handshake,
        limits: PeerCodecLimits,
    ) -> Result<Self, PeerSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        if remote.info_hash != handshake.info_hash {
            return Err(PeerSessionError::InfoHashMismatch);
        }
        timeout(HANDSHAKE_TIMEOUT, stream.write_all(&handshake.encode()))
            .await
            .map_err(|_| PeerSessionError::HandshakeTimeout)??;
        Ok(Self::start_session(stream, remote, limits))
    }

    async fn negotiate<S>(
        mut stream: S,
        handshake: Handshake,
        limits: PeerCodecLimits,
    ) -> Result<Self, PeerSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        timeout(HANDSHAKE_TIMEOUT, stream.write_all(&handshake.encode()))
            .await
            .map_err(|_| PeerSessionError::HandshakeTimeout)??;
        let mut incoming = [0_u8; super::HANDSHAKE_BYTES];
        timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut incoming))
            .await
            .map_err(|_| PeerSessionError::HandshakeTimeout)??;
        let remote = Handshake::decode(&incoming)?;
        if remote.info_hash != handshake.info_hash {
            return Err(PeerSessionError::InfoHashMismatch);
        }

        Ok(Self::start_session(stream, remote, limits))
    }

    fn start_session<S>(stream: S, remote: Handshake, limits: PeerCodecLimits) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (reader, writer) = tokio::io::split(stream);
        let (command_sender, command_receiver) = mpsc::channel(COMMAND_CAPACITY);
        let (event_sender, event_receiver) = mpsc::channel(EVENT_CAPACITY);
        let cancellation = CancellationToken::new();
        let reader_cancellation = cancellation.child_token();
        let writer_cancellation = cancellation.child_token();
        let reader_events = event_sender.clone();

        tokio::spawn(async move {
            if event_sender
                .send(PeerEvent::Connected {
                    peer_id: remote.peer_id,
                })
                .await
                .is_err()
            {
                return;
            }
            if let Err(error) =
                read_loop(reader, reader_events.clone(), reader_cancellation, limits).await
            {
                let _result_ignored = reader_events
                    .send(PeerEvent::Failed(error.to_string()))
                    .await;
            } else {
                let _result_ignored = reader_events.send(PeerEvent::Disconnected).await;
            }
        });
        tokio::spawn(async move {
            if let Err(error) = write_loop(writer, command_receiver, writer_cancellation).await {
                debug!(%error, "peer writer stopped");
            }
        });

        Self {
            commands: command_sender,
            events: event_receiver,
            cancellation,
            remote_reserved: remote.reserved,
            remote_peer_id: remote.peer_id,
        }
    }

    pub async fn send(&self, message: PeerMessage) -> Result<(), PeerSessionError> {
        self.sender().send(message).await
    }

    #[must_use]
    pub fn sender(&self) -> PeerSender {
        PeerSender {
            commands: self.commands.clone(),
        }
    }

    pub async fn next_event(&mut self) -> Option<PeerEvent> {
        self.events.recv().await
    }

    #[must_use]
    pub const fn remote_supports_extensions(&self) -> bool {
        self.remote_reserved[5] & 0x10 != 0
    }

    #[must_use]
    pub const fn remote_peer_id(&self) -> PeerId {
        self.remote_peer_id
    }

    #[must_use]
    pub const fn remote_reserved(&self) -> [u8; 8] {
        self.remote_reserved
    }

    pub fn shutdown(&self) {
        self.cancellation.cancel();
    }
}

impl PeerSender {
    pub async fn send(&self, message: PeerMessage) -> Result<(), PeerSessionError> {
        let (written, acknowledgement) = oneshot::channel();
        self.commands
            .send(PeerCommand { message, written })
            .await
            .map_err(|_| PeerSessionError::Closed)?;
        acknowledgement.await.map_err(|_| PeerSessionError::Closed)
    }
}

impl Drop for PeerConnection {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

async fn read_loop<S>(
    mut reader: ReadHalf<S>,
    events: mpsc::Sender<PeerEvent>,
    cancellation: CancellationToken,
    limits: PeerCodecLimits,
) -> Result<(), PeerSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buffered = BytesMut::with_capacity(READ_CHUNK_BYTES * 2);
    let mut scratch = vec![0_u8; READ_CHUNK_BYTES];
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return Ok(()),
            result = reader.read(&mut scratch) => {
                let read = result?;
                if read == 0 {
                    return Ok(());
                }
                let maximum_buffered = limits.frame_bytes.checked_add(4).ok_or(
                    PeerCodecError::FrameLimit {
                        actual: usize::MAX,
                        maximum: limits.frame_bytes,
                    },
                )?;
                if buffered.len().saturating_add(read) > maximum_buffered {
                    return Err(PeerCodecError::FrameLimit {
                        actual: buffered.len().saturating_add(read),
                        maximum: limits.frame_bytes,
                    }.into());
                }
                buffered.extend_from_slice(&scratch[..read]);
                while let Some(message) = decode_message(&mut buffered, limits)? {
                    events.send(PeerEvent::Message(message)).await.map_err(|_| PeerSessionError::Closed)?;
                }
            }
        }
    }
}

async fn write_loop<S>(
    mut writer: WriteHalf<S>,
    mut commands: mpsc::Receiver<PeerCommand>,
    cancellation: CancellationToken,
) -> Result<(), PeerSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            () = cancellation.cancelled() => {
                writer.shutdown().await?;
                return Ok(());
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    writer.shutdown().await?;
                    return Ok(());
                };
                let encoded = encode_message(&command.message)?;
                writer.write_all(&encoded).await?;
                let _result_ignored = command.written.send(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use dendrite_core::Sha1Hash;
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };

    use super::*;

    fn handshake(peer: u8) -> Handshake {
        Handshake {
            reserved: [0; 8],
            info_hash: Sha1Hash::from_bytes([3; 20]),
            peer_id: PeerId::from_bytes([peer; 20]),
        }
    }

    #[tokio::test]
    async fn sessions_negotiate_and_exchange_fifo_messages()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let accepting = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            PeerConnection::accept(stream, handshake(2), PeerCodecLimits::default()).await
        });
        let client =
            PeerConnection::connect(address, handshake(1), PeerCodecLimits::default()).await?;
        let mut server = accepting.await??;
        assert!(!client.remote_supports_extensions());
        assert!(!server.remote_supports_extensions());

        client.send(PeerMessage::Interested).await?;
        client.send(PeerMessage::Have(7)).await?;
        assert!(matches!(
            server.next_event().await,
            Some(PeerEvent::Connected { .. })
        ));
        assert_eq!(
            server.next_event().await,
            Some(PeerEvent::Message(PeerMessage::Interested))
        );
        assert_eq!(
            server.next_event().await,
            Some(PeerEvent::Message(PeerMessage::Have(7)))
        );
        client.shutdown();
        server.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn exposes_remote_extension_support() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let accepting = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut server_handshake = handshake(2);
            server_handshake.reserved[5] |= 0x10;
            PeerConnection::accept(stream, server_handshake, PeerCodecLimits::default()).await
        });
        let client =
            PeerConnection::connect(address, handshake(1), PeerCodecLimits::default()).await?;
        let server = accepting.await??;

        assert!(client.remote_supports_extensions());
        assert!(!server.remote_supports_extensions());
        client.shutdown();
        server.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn encrypted_sessions_negotiate_peer_wire_messages()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let accepting = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let (encrypted, selected) = crate::mse::respond(stream, &[handshake(2).info_hash])
                .await
                .map_err(|error| PeerSessionError::Encryption(error.to_string()))?;
            if selected != handshake(2).info_hash {
                return Err(PeerSessionError::InfoHashMismatch);
            }
            PeerConnection::from_stream(encrypted, handshake(2), PeerCodecLimits::default()).await
        });
        let client =
            PeerConnection::connect_encrypted(address, handshake(1), PeerCodecLimits::default())
                .await?;
        let mut server = accepting.await??;
        client.send(PeerMessage::Interested).await?;
        loop {
            match server.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Interested)) => break,
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => {
                    return Err("encrypted peer disconnected".into());
                }
                _ => {}
            }
        }
        client.shutdown();
        server.shutdown();
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn slowloris_handshake_is_evicted_at_the_deadline()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let slowloris = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut incoming = [0_u8; super::super::HANDSHAKE_BYTES];
            stream.read_exact(&mut incoming).await?;
            stream.write_all(&handshake(2).encode()[..1]).await?;
            std::future::pending::<()>().await;
            Ok::<(), io::Error>(())
        });

        let result =
            PeerConnection::connect(address, handshake(1), PeerCodecLimits::default()).await;
        assert!(matches!(result, Err(PeerSessionError::HandshakeTimeout)));
        slowloris.abort();
        let _result_ignored = slowloris.await;
        Ok(())
    }
}
