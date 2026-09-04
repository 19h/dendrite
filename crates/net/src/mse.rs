//! `BitTorrent` Message Stream Encryption (MSE/PE) with RC4-drop1024.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf as _, Bytes};
use crypto_bigint::{
    Encoding, U768,
    modular::runtime_mod::{DynResidue, DynResidueParams},
};
use dendrite_core::Sha1Hash;
use sha1::{Digest as _, Sha1};
use subtle::ConstantTimeEq as _;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, ReadBuf};

const KEY_BYTES: usize = 96;
const MAX_PADDING: usize = 512;
const MAX_INITIAL_PAYLOAD: usize = 4 * 1024;
const CRYPTO_RC4: u32 = 0x02;
const VC: [u8; 8] = [0; 8];
const PRIME: U768 = U768::from_be_hex(
    "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563",
);

#[derive(Debug, Error)]
pub enum MseError {
    #[error("MSE I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("MSE handshake is malformed")]
    Malformed,
    #[error("MSE peer requested an unknown info hash")]
    UnknownInfoHash,
    #[error("MSE peer did not offer RC4 encryption")]
    NoEncryption,
}

#[derive(Clone)]
struct Rc4 {
    state: [u8; 256],
    i: u8,
    j: u8,
}

impl Rc4 {
    fn new(key: &[u8]) -> Self {
        let mut state = [0_u8; 256];
        for (index, value) in state.iter_mut().enumerate() {
            *value = u8::try_from(index).unwrap_or_default();
        }
        let mut j = 0_u8;
        for index in 0..state.len() {
            j = j
                .wrapping_add(state[index])
                .wrapping_add(key[index % key.len()]);
            state.swap(index, usize::from(j));
        }
        Self { state, i: 0, j: 0 }
    }

    /// Applies the keystream in place. The cipher indices stay in registers
    /// for the whole call and the output is combined eight bytes at a time; the
    /// per-byte loop with struct fields stored on every step was the
    /// daemon's largest CPU cost once encrypted peers dominated inbound.
    fn apply(&mut self, bytes: &mut [u8]) {
        let mut i = self.i;
        let mut j = self.j;
        let state = &mut self.state;
        let mut chunks = bytes.chunks_exact_mut(8);
        for chunk in &mut chunks {
            let mut keystream = [0_u8; 8];
            for key_byte in &mut keystream {
                i = i.wrapping_add(1);
                let si = state[usize::from(i)];
                j = j.wrapping_add(si);
                let sj = state[usize::from(j)];
                state[usize::from(i)] = sj;
                state[usize::from(j)] = si;
                *key_byte = state[usize::from(si.wrapping_add(sj))];
            }
            let word = u64::from_ne_bytes(<[u8; 8]>::try_from(&*chunk).unwrap_or_default())
                ^ u64::from_ne_bytes(keystream);
            chunk.copy_from_slice(&word.to_ne_bytes());
        }
        for byte in chunks.into_remainder() {
            i = i.wrapping_add(1);
            let si = state[usize::from(i)];
            j = j.wrapping_add(si);
            let sj = state[usize::from(j)];
            state[usize::from(i)] = sj;
            state[usize::from(j)] = si;
            *byte ^= state[usize::from(si.wrapping_add(sj))];
        }
        self.i = i;
        self.j = j;
    }

    fn discard(&mut self) {
        self.apply(&mut [0_u8; 1024]);
    }
}

struct KeyExchange {
    private: U768,
    public: [u8; KEY_BYTES],
}

impl KeyExchange {
    fn generate() -> Self {
        let secret: [u8; 20] = rand::random();
        let mut private_bytes = [0_u8; KEY_BYTES];
        private_bytes[KEY_BYTES - secret.len()..].copy_from_slice(&secret);
        private_bytes[KEY_BYTES - 1] |= 1;
        let private = U768::from_be_slice(&private_bytes);
        let parameters = DynResidueParams::new(&PRIME);
        let generator = DynResidue::new(&U768::from(2_u8), parameters);
        let public = generator.pow(&private).retrieve().to_be_bytes();
        Self { private, public }
    }

    fn shared(&self, remote: [u8; KEY_BYTES]) -> Result<[u8; KEY_BYTES], MseError> {
        if remote[..KEY_BYTES - 1].iter().all(|byte| *byte == 0) && remote[KEY_BYTES - 1] <= 1 {
            return Err(MseError::Malformed);
        }
        let remote = U768::from_be_slice(&remote);
        if remote >= PRIME {
            return Err(MseError::Malformed);
        }
        let parameters = DynResidueParams::new(&PRIME);
        Ok(DynResidue::new(&remote, parameters)
            .pow(&self.private)
            .retrieve()
            .to_be_bytes())
    }
}

/// Negotiate an outbound encrypted stream for one known torrent.
pub async fn initiate<S>(mut stream: S, info_hash: Sha1Hash) -> Result<MseStream<S>, MseError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let exchange = KeyExchange::generate();
    stream.write_all(&exchange.public).await?;
    let mut remote = [0_u8; KEY_BYTES];
    stream.read_exact(&mut remote).await?;
    let shared = exchange.shared(remote)?;
    let (mut encrypt, mut decrypt) = ciphers(&shared, info_hash, true);

    let req1 = digest(&[b"req1", &shared]);
    let req2 = digest(&[b"req2", info_hash.as_bytes()]);
    let req3 = digest(&[b"req3", &shared]);
    let mut identity = [0_u8; 20];
    for (output, (left, right)) in identity.iter_mut().zip(req2.iter().zip(req3)) {
        *output = *left ^ right;
    }
    let mut negotiation = [0_u8; 16];
    negotiation[..8].copy_from_slice(&VC);
    negotiation[8..12].copy_from_slice(&CRYPTO_RC4.to_be_bytes());
    encrypt.apply(&mut negotiation);
    stream.write_all(&req1).await?;
    stream.write_all(&identity).await?;
    stream.write_all(&negotiation).await?;

    let mut encrypted_vc = VC;
    decrypt.clone().apply(&mut encrypted_vc);
    scan_for(&mut stream, &encrypted_vc).await?;
    decrypt.apply(&mut encrypted_vc);
    if encrypted_vc != VC {
        return Err(MseError::Malformed);
    }
    let mut response = [0_u8; 6];
    stream.read_exact(&mut response).await?;
    decrypt.apply(&mut response);
    if u32::from_be_bytes(response[..4].try_into().map_err(|_| MseError::Malformed)?) != CRYPTO_RC4
    {
        return Err(MseError::NoEncryption);
    }
    let padding = usize::from(u16::from_be_bytes([response[4], response[5]]));
    read_encrypted_padding(&mut stream, &mut decrypt, padding).await?;
    Ok(MseStream::new(stream, decrypt, encrypt, Bytes::new()))
}

/// Negotiate an inbound stream, selecting its torrent without exposing the
/// plaintext info hash on the wire.
pub async fn respond<S>(
    stream: S,
    candidates: &[Sha1Hash],
) -> Result<(MseStream<S>, Sha1Hash), MseError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    respond_prefixed(stream, &[], candidates).await
}

/// Respond after a listener has already consumed a bounded prefix while
/// distinguishing encrypted traffic from the plaintext peer handshake.
pub async fn respond_prefixed<S>(
    mut stream: S,
    prefix: &[u8],
    candidates: &[Sha1Hash],
) -> Result<(MseStream<S>, Sha1Hash), MseError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if prefix.len() > KEY_BYTES {
        return Err(MseError::Malformed);
    }
    let mut remote = [0_u8; KEY_BYTES];
    remote[..prefix.len()].copy_from_slice(prefix);
    stream.read_exact(&mut remote[prefix.len()..]).await?;
    let exchange = KeyExchange::generate();
    stream.write_all(&exchange.public).await?;
    let shared = exchange.shared(remote)?;
    let req1 = digest(&[b"req1", &shared]);
    scan_for(&mut stream, &req1).await?;
    let mut obfuscated = [0_u8; 20];
    stream.read_exact(&mut obfuscated).await?;
    let req3 = digest(&[b"req3", &shared]);
    let info_hash = candidates
        .iter()
        .copied()
        .find(|candidate| {
            let req2 = digest(&[b"req2", candidate.as_bytes()]);
            let expected: Vec<_> = req2.iter().zip(req3).map(|(a, b)| *a ^ b).collect();
            expected.ct_eq(&obfuscated).into()
        })
        .ok_or(MseError::UnknownInfoHash)?;
    let (mut encrypt, mut decrypt) = ciphers(&shared, info_hash, false);
    let mut negotiation = [0_u8; 14];
    stream.read_exact(&mut negotiation).await?;
    decrypt.apply(&mut negotiation);
    if negotiation[..8] != VC
        || u32::from_be_bytes(
            negotiation[8..12]
                .try_into()
                .map_err(|_| MseError::Malformed)?,
        ) & CRYPTO_RC4
            == 0
    {
        return Err(MseError::NoEncryption);
    }
    let padding = usize::from(u16::from_be_bytes([negotiation[12], negotiation[13]]));
    read_encrypted_padding(&mut stream, &mut decrypt, padding).await?;
    let mut initial_length = [0_u8; 2];
    stream.read_exact(&mut initial_length).await?;
    decrypt.apply(&mut initial_length);
    let initial_length = usize::from(u16::from_be_bytes(initial_length));
    if initial_length > MAX_INITIAL_PAYLOAD {
        return Err(MseError::Malformed);
    }
    let mut initial = vec![0_u8; initial_length];
    stream.read_exact(&mut initial).await?;
    decrypt.apply(&mut initial);

    let mut response = [0_u8; 14];
    response[..8].copy_from_slice(&VC);
    response[8..12].copy_from_slice(&CRYPTO_RC4.to_be_bytes());
    encrypt.apply(&mut response);
    stream.write_all(&response).await?;
    Ok((
        MseStream::new(stream, decrypt, encrypt, Bytes::from(initial)),
        info_hash,
    ))
}

fn ciphers(shared: &[u8; KEY_BYTES], info_hash: Sha1Hash, initiator: bool) -> (Rc4, Rc4) {
    let outbound_label: &[u8] = if initiator { b"keyA" } else { b"keyB" };
    let inbound_label: &[u8] = if initiator { b"keyB" } else { b"keyA" };
    let mut encrypt = Rc4::new(&digest(&[outbound_label, shared, info_hash.as_bytes()]));
    let mut decrypt = Rc4::new(&digest(&[inbound_label, shared, info_hash.as_bytes()]));
    encrypt.discard();
    decrypt.discard();
    (encrypt, decrypt)
}

fn digest(parts: &[&[u8]]) -> [u8; 20] {
    let mut digest = Sha1::new();
    for part in parts {
        digest.update(part);
    }
    digest.finalize().into()
}

async fn scan_for<S>(stream: &mut S, marker: &[u8]) -> Result<(), MseError>
where
    S: AsyncRead + Unpin,
{
    let mut window = Vec::with_capacity(marker.len());
    for _ in 0..MAX_PADDING + marker.len() {
        let byte = stream.read_u8().await?;
        if window.len() == marker.len() {
            window.remove(0);
        }
        window.push(byte);
        if window == marker {
            return Ok(());
        }
    }
    Err(MseError::Malformed)
}

async fn read_encrypted_padding<S>(
    stream: &mut S,
    cipher: &mut Rc4,
    length: usize,
) -> Result<(), MseError>
where
    S: AsyncRead + Unpin,
{
    if length > MAX_PADDING {
        return Err(MseError::Malformed);
    }
    let mut padding = vec![0_u8; length];
    stream.read_exact(&mut padding).await?;
    cipher.apply(&mut padding);
    Ok(())
}

/// An asynchronous stream that transparently applies the negotiated MSE
/// ciphers and preserves any initial application bytes from the handshake.
pub struct MseStream<S> {
    inner: S,
    decrypt: Rc4,
    encrypt: Rc4,
    prefix: Bytes,
    pending: Vec<u8>,
    pending_offset: usize,
}

impl<S> MseStream<S> {
    fn new(inner: S, decrypt: Rc4, encrypt: Rc4, prefix: Bytes) -> Self {
        Self {
            inner,
            decrypt,
            encrypt,
            prefix,
            pending: Vec::new(),
            pending_offset: 0,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for MseStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.prefix.is_empty() && buffer.remaining() > 0 {
            let length = self.prefix.len().min(buffer.remaining());
            buffer.put_slice(&self.prefix[..length]);
            self.prefix.advance(length);
            return Poll::Ready(Ok(()));
        }
        let before = buffer.filled().len();
        match Pin::new(&mut self.inner).poll_read(context, buffer) {
            Poll::Ready(Ok(())) => {
                self.decrypt.apply(&mut buffer.filled_mut()[before..]);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for MseStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.as_mut().poll_pending(context) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        let mut encrypted = std::mem::take(&mut self.pending);
        encrypted.clear();
        encrypted.extend_from_slice(buffer);
        self.encrypt.apply(&mut encrypted);
        self.pending = encrypted;
        let accepted = buffer.len();
        match self.as_mut().poll_pending(context) {
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) | Poll::Pending => Poll::Ready(Ok(accepted)),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match self.as_mut().poll_pending(context) {
            Poll::Ready(Ok(())) => Pin::new(&mut self.inner).poll_flush(context),
            other => other,
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match self.as_mut().poll_pending(context) {
            Poll::Ready(Ok(())) => Pin::new(&mut self.inner).poll_shutdown(context),
            other => other,
        }
    }
}

impl<S: AsyncWrite + Unpin> MseStream<S> {
    fn poll_pending(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        while self.pending_offset < self.pending.len() {
            let this = self.as_mut().get_mut();
            let offset = this.pending_offset;
            let written =
                match Pin::new(&mut this.inner).poll_write(context, &this.pending[offset..]) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write encrypted stream",
                        )));
                    }
                    Poll::Ready(Ok(written)) => written,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                };
            self.pending_offset += written;
        }
        self.pending.clear();
        self.pending_offset = 0;
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_the_mse_diffie_hellman_modulus() {
        let bytes = PRIME.to_be_bytes();
        assert_eq!(
            &bytes[KEY_BYTES - 12..],
            &[0xa6, 0x3a, 0x36, 0x21, 0, 0, 0, 0, 0, 0x09, 0x05, 0x63]
        );
    }

    #[tokio::test]
    async fn initiator_and_responder_exchange_encrypted_stream_data()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let hash = Sha1Hash::from_bytes([0x5a; 20]);
        let (initiator_io, responder_io) = tokio::io::duplex(16 * 1024);
        let responder = tokio::spawn(async move { respond(responder_io, &[hash]).await });
        let mut initiator = initiate(initiator_io, hash).await?;
        let (mut responder, selected) = responder.await??;
        assert_eq!(selected, hash);
        initiator.write_all(b"encrypted request").await?;
        initiator.flush().await?;
        let mut request = [0_u8; 17];
        responder.read_exact(&mut request).await?;
        assert_eq!(&request, b"encrypted request");
        responder.write_all(b"encrypted response").await?;
        responder.flush().await?;
        let mut response = [0_u8; 18];
        initiator.read_exact(&mut response).await?;
        assert_eq!(&response, b"encrypted response");
        Ok(())
    }
}
