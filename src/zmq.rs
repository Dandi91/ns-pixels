//! Minimal no-std ZeroMQ subscriber (ZMTP 3.0, NULL mechanism).
//!
//! Implements only what is needed to connect a SUB socket to a PUB peer,
//! subscribe to topics, and receive messages. Anything else (REQ/REP, CURVE,
//! PLAIN, monitoring, multipart sending, etc.) is intentionally absent.
//!
//! Usage sketch:
//! ```ignore
//! let mut socket = TcpSocket::new(stack, &mut rx, &mut tx);
//! socket.connect((peer_ip, 5556)).await.unwrap();
//! let mut sub = zmq::Subscriber::new(socket, 64 * 1024).await?;
//! sub.subscribe(b"topic.").await?;
//! let frames = sub.recv().await?;
//! ```
extern crate alloc;

use alloc::vec;
use alloc::vec::Vec;

use embedded_io_async::{Read, Write};

const FLAG_MORE: u8 = 0x01;
const FLAG_LONG: u8 = 0x02;
const FLAG_COMMAND: u8 = 0x04;

const GREETING: [u8; 64] = {
    let mut g = [0u8; 64];
    // signature: 0xFF, 8 zero bytes, 0x7F
    g[0] = 0xFF;
    g[9] = 0x7F;
    // version 3.0 — using minor 0 keeps SUBSCRIBE in pseudo-frame form,
    // which every libzmq peer accepts regardless of its own version.
    g[10] = 0x03;
    g[11] = 0x00;
    // mechanism "NULL", padded to 20 bytes
    g[12] = b'N';
    g[13] = b'U';
    g[14] = b'L';
    g[15] = b'L';
    // as-server = 0, filler = 31 zero bytes (already zero)
    g
};

#[derive(Debug)]
pub enum Error<E> {
    Io(E),
    UnexpectedEof,
    InvalidGreeting,
    UnsupportedMechanism,
    UnexpectedCommand,
    FrameTooLarge,
}

pub struct Subscriber<S> {
    socket: S,
    max_frame_size: usize,
}

impl<S: Read + Write> Subscriber<S> {
    /// Perform the ZMTP greeting + NULL handshake over an already-connected
    /// stream. `max_frame_size` caps the size of any single frame we will
    /// allocate to receive (defends against a malicious or buggy peer).
    pub async fn new(socket: S, max_frame_size: usize) -> Result<Self, Error<S::Error>> {
        let mut sub = Self {
            socket,
            max_frame_size,
        };
        sub.handshake().await?;
        Ok(sub)
    }

    async fn handshake(&mut self) -> Result<(), Error<S::Error>> {
        self.socket.write_all(&GREETING).await.map_err(Error::Io)?;
        self.socket.flush().await.map_err(Error::Io)?;

        let mut peer = [0u8; 64];
        self.read_exact(&mut peer).await?;
        if peer[0] != 0xFF || peer[9] != 0x7F {
            return Err(Error::InvalidGreeting);
        }
        if peer[10] < 3 {
            return Err(Error::InvalidGreeting);
        }
        if &peer[12..16] != b"NULL" {
            return Err(Error::UnsupportedMechanism);
        }

        // NULL handshake: each side sends a READY command, then the
        // connection is open for traffic.
        let ready = build_ready_body();
        self.send_frame(FLAG_COMMAND, &ready).await?;

        let (flags, _body) = self.recv_frame().await?;
        if flags & FLAG_COMMAND == 0 {
            return Err(Error::UnexpectedCommand);
        }
        Ok(())
    }

    /// Subscribe to a topic prefix. An empty topic matches every message.
    pub async fn subscribe(&mut self, topic: &[u8]) -> Result<(), Error<S::Error>> {
        self.send_subscription(0x01, topic).await
    }

    pub async fn unsubscribe(&mut self, topic: &[u8]) -> Result<(), Error<S::Error>> {
        self.send_subscription(0x00, topic).await
    }

    async fn send_subscription(&mut self, op: u8, topic: &[u8]) -> Result<(), Error<S::Error>> {
        // ZMTP 3.0 carries (un)subscribe as a regular message frame whose body
        // is op-byte || topic. ZMTP 3.1 has a dedicated SUBSCRIBE command, but
        // libzmq accepts the 3.0 form on either version.
        let mut body = Vec::with_capacity(topic.len() + 1);
        body.push(op);
        body.extend_from_slice(topic);
        self.send_frame(0, &body).await
    }

    /// Receive one logical message, returning its frames in order. Single-part
    /// messages produce a one-element vector. Command frames (PING, etc.) are
    /// silently skipped — we don't need them for SUB.
    pub async fn recv(&mut self) -> Result<Vec<Vec<u8>>, Error<S::Error>> {
        let mut frames = Vec::new();
        loop {
            let (flags, body) = self.recv_frame().await?;
            if flags & FLAG_COMMAND != 0 {
                continue;
            }
            let more = flags & FLAG_MORE != 0;
            frames.push(body);
            if !more {
                return Ok(frames);
            }
        }
    }

    async fn send_frame(&mut self, flags: u8, body: &[u8]) -> Result<(), Error<S::Error>> {
        if body.len() <= u8::MAX as usize {
            let header = [flags & !FLAG_LONG, body.len() as u8];
            self.socket.write_all(&header).await.map_err(Error::Io)?;
        } else {
            let mut header = [0u8; 9];
            header[0] = flags | FLAG_LONG;
            header[1..9].copy_from_slice(&(body.len() as u64).to_be_bytes());
            self.socket.write_all(&header).await.map_err(Error::Io)?;
        }
        self.socket.write_all(body).await.map_err(Error::Io)?;
        self.socket.flush().await.map_err(Error::Io)?;
        Ok(())
    }

    async fn recv_frame(&mut self) -> Result<(u8, Vec<u8>), Error<S::Error>> {
        let mut flag_byte = [0u8; 1];
        self.read_exact(&mut flag_byte).await?;
        let flags = flag_byte[0];

        let len = if flags & FLAG_LONG != 0 {
            let mut buf = [0u8; 8];
            self.read_exact(&mut buf).await?;
            u64::from_be_bytes(buf) as usize
        } else {
            let mut buf = [0u8; 1];
            self.read_exact(&mut buf).await?;
            buf[0] as usize
        };

        if len > self.max_frame_size {
            return Err(Error::FrameTooLarge);
        }

        let mut body = vec![0u8; len];
        self.read_exact(&mut body).await?;
        Ok((flags, body))
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Error<S::Error>> {
        let mut filled = 0;
        while filled < buf.len() {
            let n = self
                .socket
                .read(&mut buf[filled..])
                .await
                .map_err(Error::Io)?;
            if n == 0 {
                return Err(Error::UnexpectedEof);
            }
            filled += n;
        }
        Ok(())
    }

    pub fn into_inner(self) -> S {
        self.socket
    }
}

fn build_ready_body() -> Vec<u8> {
    // READY command body layout:
    //   short-name-length (1 byte) || command-name "READY"
    //   then one or more metadata properties:
    //     name-length (1 byte) || name || value-length (4 bytes BE) || value
    let mut body = Vec::with_capacity(32);
    body.push(5);
    body.extend_from_slice(b"READY");
    push_property(&mut body, b"Socket-Type", b"SUB");
    body
}

fn push_property(out: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    out.push(name.len() as u8);
    out.extend_from_slice(name);
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
}
