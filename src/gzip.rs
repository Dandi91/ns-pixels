//! Minimal helpers for working with gzip-wrapped streams.
//!
//! `miniz_oxide` only inflates raw deflate / zlib, so we strip the RFC 1952
//! gzip envelope ourselves before feeding the body to the inflater.

/// Skip an RFC 1952 gzip header. Returns the number of bytes consumed, or
/// `None` if the header is malformed/truncated.
pub fn skip_header(buf: &[u8]) -> Option<usize> {
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
