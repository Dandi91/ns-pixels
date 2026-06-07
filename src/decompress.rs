//! Gzip → in-memory inflater backed by a PSRAM scratch buffer.
//!
//! Owns a long-lived [`InflateState`] and output buffer (both allocated in
//! external PSRAM), and exposes a single call that takes a gzip-framed payload
//! and yields the decompressed bytes as a borrowed `&str`.

use alloc::boxed::Box;
use esp_alloc::ExternalMemory;
use miniz_oxide::{
    DataFormat, MZFlush, MZStatus,
    inflate::stream::{InflateState, inflate},
};

#[derive(Debug)]
pub enum Error {
    BadGzipHeader,
    Overflow,
    Inflate(miniz_oxide::MZError),
    NotUtf8(core::str::Utf8Error),
}

pub struct Decompressor {
    state: Box<InflateState, ExternalMemory>,
    buf: Box<[u8], ExternalMemory>,
}

impl Decompressor {
    /// Allocate the inflater and a `buf_len`-byte output buffer in PSRAM.
    pub fn new(buf_len: usize) -> Self {
        let state = Box::<InflateState, _>::new_in(InflateState::new(DataFormat::Raw), ExternalMemory);
        let buf = Box::<[u8], _>::new_uninit_slice_in(buf_len, ExternalMemory);
        // SAFETY: u8 has no validity invariants; we only ever read the prefix
        // we just wrote during inflate.
        let buf = unsafe { buf.assume_init() };
        Self { state, buf }
    }

    /// Strip the gzip header from `payload`, inflate the body into the PSRAM
    /// buffer, and return the result as a UTF-8 string slice.
    pub fn inflate_gzip(&mut self, payload: &[u8]) -> Result<&str, Error> {
        let header_len = Self::skip_header(payload).ok_or(Error::BadGzipHeader)?;

        self.state.reset(DataFormat::Raw);
        let mut input = &payload[header_len..];
        let mut written = 0usize;

        loop {
            if written == self.buf.len() {
                return Err(Error::Overflow);
            }
            let last = input.is_empty();
            let flush = if last { MZFlush::Finish } else { MZFlush::None };
            let res = inflate(&mut self.state, input, &mut self.buf[written..], flush);
            input = &input[res.bytes_consumed..];
            written += res.bytes_written;
            match res.status {
                Ok(MZStatus::StreamEnd) => break,
                Ok(_) => {
                    if res.bytes_written == 0 && res.bytes_consumed == 0 {
                        // No progress — treat as end of useful data.
                        break;
                    }
                }
                Err(e) => return Err(Error::Inflate(e)),
            }
        }

        core::str::from_utf8(&self.buf[..written]).map_err(Error::NotUtf8)
    }

    fn skip_header(buf: &[u8]) -> Option<usize> {
        if buf.len() < 10 || buf[0] != 0x1f || buf[1] != 0x8b || buf[2] != 8 {
            return None;
        }
        let flags = buf[3];
        let mut p = 10;
        if flags & 0x04 != 0 {
            // FEXTRA: 2-byte length, then payload
            if buf.len() < p + 2 {
                return None;
            }
            let xlen = u16::from_le_bytes([buf[p], buf[p + 1]]) as usize;
            p += 2 + xlen;
            if buf.len() < p {
                return None;
            }
        }
        if flags & 0x08 != 0 {
            // FNAME: NUL-terminated
            let end = buf[p..].iter().position(|&b| b == 0)?;
            p += end + 1;
        }
        if flags & 0x10 != 0 {
            // FCOMMENT: NUL-terminated
            let end = buf[p..].iter().position(|&b| b == 0)?;
            p += end + 1;
        }
        if flags & 0x02 != 0 {
            // FHCRC
            if buf.len() < p + 2 {
                return None;
            }
            p += 2;
        }
        Some(p)
    }
}
