//! Streaming, schema-specific XML scanner for the NDOV
//! `NStreinpositiesInterface5` feed.
//!
//! It is *not* a generic XML parser. It exists to pull three values out of
//! each `<tns3:TreinLocation>` element with O(1) state and no buffering of
//! the whole document:
//!
//!   * `<tns3:TreinNummer>` text
//!   * the *first* `<tns:TreinMaterieelDelen>` child's `<tns:Longitude>`
//!     and `<tns:Latitude>` text
//!
//! Subsequent `TreinMaterieelDelen` siblings (additional train parts) are
//! skipped — the assumption is that all parts of one train are roughly
//! co-located.
//!
//! The parser is byte-driven: feed arbitrary chunks via [`Parser::feed`]
//! and it invokes the callback once per fully-parsed `TreinLocation`.

const NAME_CAP: usize = 32;
const TEXT_CAP: usize = 24;

#[derive(Debug, Clone, Copy)]
pub struct Train {
    pub number: u32,
    pub lon: f32,
    pub lat: f32,
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Tok {
    Text,
    TagOpen,    // just after '<'
    TagName,    // accumulating element name
    InTag,      // past name, scanning attributes
    SelfSlash,  // saw '/' inside open tag, expect '>'
    Pi,         // inside <? ... ?>
    PiQ,        // saw '?' inside PI
    Bang,       // inside <! ... > (DOCTYPE/comment) — naive skip to '>'
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Capture {
    None,
    Nummer,
    Lon,
    Lat,
}

struct Buf<const N: usize> {
    data: [u8; N],
    len: usize,
    overflow: bool,
}

impl<const N: usize> Buf<N> {
    const fn new() -> Self {
        Self {
            data: [0; N],
            len: 0,
            overflow: false,
        }
    }
    fn clear(&mut self) {
        self.len = 0;
        self.overflow = false;
    }
    fn push(&mut self, b: u8) {
        if self.len < N {
            self.data[self.len] = b;
            self.len += 1;
        } else {
            self.overflow = true;
        }
    }
    fn as_slice(&self) -> &[u8] {
        &self.data[..self.len]
    }
}

pub struct Parser {
    tok: Tok,
    is_close: bool,

    name: Buf<NAME_CAP>,
    text: Buf<TEXT_CAP>,

    in_treinloc: bool,
    seen_first_part: bool,
    in_first_part: bool,

    capture: Capture,
    nummer: Option<u32>,
    lon: Option<f32>,
    lat: Option<f32>,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser {
    pub const fn new() -> Self {
        Self {
            tok: Tok::Text,
            is_close: false,
            name: Buf::new(),
            text: Buf::new(),
            in_treinloc: false,
            seen_first_part: false,
            in_first_part: false,
            capture: Capture::None,
            nummer: None,
            lon: None,
            lat: None,
        }
    }

    pub fn feed<F: FnMut(Train)>(&mut self, chunk: &[u8], mut on_train: F) {
        for &b in chunk {
            self.step(b, &mut on_train);
        }
    }

    fn step<F: FnMut(Train)>(&mut self, b: u8, on_train: &mut F) {
        match self.tok {
            Tok::Text => {
                if b == b'<' {
                    self.tok = Tok::TagOpen;
                    self.is_close = false;
                    self.name.clear();
                } else if self.capture != Capture::None {
                    self.text.push(b);
                }
            }
            Tok::TagOpen => match b {
                b'/' => {
                    self.is_close = true;
                    self.tok = Tok::TagName;
                }
                b'?' => self.tok = Tok::Pi,
                b'!' => self.tok = Tok::Bang,
                _ => {
                    self.name.push(b);
                    self.tok = Tok::TagName;
                }
            },
            Tok::TagName => {
                if b == b'>' {
                    self.finish_tag(false, on_train);
                    self.tok = Tok::Text;
                } else if b == b'/' {
                    self.tok = Tok::SelfSlash;
                } else if is_space(b) {
                    self.tok = Tok::InTag;
                } else {
                    self.name.push(b);
                }
            }
            Tok::InTag => {
                if b == b'>' {
                    self.finish_tag(false, on_train);
                    self.tok = Tok::Text;
                } else if b == b'/' {
                    self.tok = Tok::SelfSlash;
                }
                // attribute bytes are ignored; quoted '>' inside attributes
                // does not occur in this feed
            }
            Tok::SelfSlash => {
                if b == b'>' {
                    self.finish_tag(true, on_train);
                    self.tok = Tok::Text;
                } else {
                    self.tok = Tok::InTag;
                }
            }
            Tok::Pi => {
                if b == b'?' {
                    self.tok = Tok::PiQ;
                }
            }
            Tok::PiQ => {
                if b == b'>' {
                    self.tok = Tok::Text;
                } else if b != b'?' {
                    self.tok = Tok::Pi;
                }
            }
            Tok::Bang => {
                // Coarse: the NDOV feed has no comments/CDATA/DOCTYPE inside
                // the body; if one ever appears we just skip to the next '>'.
                if b == b'>' {
                    self.tok = Tok::Text;
                }
            }
        }
    }

    fn finish_tag<F: FnMut(Train)>(&mut self, self_closing: bool, on_train: &mut F) {
        if self.name.overflow {
            return;
        }
        let mut tmp = [0u8; NAME_CAP];
        let local_src = local_name(self.name.as_slice());
        let n = local_src.len();
        tmp[..n].copy_from_slice(local_src);
        let local = &tmp[..n];
        if self.is_close {
            self.handle_close(local, on_train);
        } else if self_closing {
            self.handle_open(local);
            self.handle_close(local, on_train);
        } else {
            self.handle_open(local);
        }
    }

    fn handle_open(&mut self, local: &[u8]) {
        match local {
            b"TreinLocation" => {
                self.in_treinloc = true;
                self.seen_first_part = false;
                self.in_first_part = false;
                self.nummer = None;
                self.lon = None;
                self.lat = None;
                self.capture = Capture::None;
            }
            b"TreinNummer" if self.in_treinloc => self.start_capture(Capture::Nummer),
            b"TreinMaterieelDelen" if self.in_treinloc && !self.seen_first_part => {
                self.in_first_part = true;
                self.seen_first_part = true;
            }
            b"Longitude" if self.in_first_part => self.start_capture(Capture::Lon),
            b"Latitude" if self.in_first_part => self.start_capture(Capture::Lat),
            _ => {}
        }
    }

    fn handle_close<F: FnMut(Train)>(&mut self, local: &[u8], on_train: &mut F) {
        match local {
            b"TreinNummer" if self.capture == Capture::Nummer => {
                self.nummer = parse_text(&self.text).and_then(|s| s.trim().parse().ok());
                self.end_capture();
            }
            b"Longitude" if self.capture == Capture::Lon => {
                self.lon = parse_text(&self.text).and_then(|s| s.trim().parse().ok());
                self.end_capture();
            }
            b"Latitude" if self.capture == Capture::Lat => {
                self.lat = parse_text(&self.text).and_then(|s| s.trim().parse().ok());
                self.end_capture();
            }
            b"TreinMaterieelDelen" if self.in_first_part => {
                self.in_first_part = false;
            }
            b"TreinLocation" if self.in_treinloc => {
                if let (Some(n), Some(lo), Some(la)) = (self.nummer, self.lon, self.lat) {
                    on_train(Train {
                        number: n,
                        lon: lo,
                        lat: la,
                    });
                }
                self.in_treinloc = false;
                self.in_first_part = false;
                self.seen_first_part = false;
                self.capture = Capture::None;
            }
            _ => {}
        }
    }

    fn start_capture(&mut self, c: Capture) {
        self.capture = c;
        self.text.clear();
    }

    fn end_capture(&mut self) {
        self.capture = Capture::None;
        self.text.clear();
    }
}

fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\r' | b'\n')
}

fn local_name(full: &[u8]) -> &[u8] {
    match full.iter().position(|&b| b == b':') {
        Some(i) => &full[i + 1..],
        None => full,
    }
}

fn parse_text(buf: &Buf<TEXT_CAP>) -> Option<&str> {
    if buf.overflow {
        return None;
    }
    core::str::from_utf8(buf.as_slice()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate alloc;
    use alloc::vec::Vec;

    fn parse_all(xml: &[u8]) -> Vec<Train> {
        let mut p = Parser::new();
        let mut out = Vec::new();
        p.feed(xml, |t| out.push(t));
        out
    }

    #[test]
    fn picks_first_part_only() {
        let xml = br#"<?xml version="1.0"?>
        <a:ArrayOfTreinLocation xmlns:a="x">
          <a:TreinLocation>
            <a:TreinNummer>6441</a:TreinNummer>
            <b:TreinMaterieelDelen xmlns:b="y">
              <b:Longitude>5.486703</b:Longitude>
              <b:Latitude>51.443425</b:Latitude>
            </b:TreinMaterieelDelen>
            <b:TreinMaterieelDelen xmlns:b="y">
              <b:Longitude>9.999</b:Longitude>
              <b:Latitude>9.999</b:Latitude>
            </b:TreinMaterieelDelen>
          </a:TreinLocation>
        </a:ArrayOfTreinLocation>"#;
        let trains = parse_all(xml);
        assert_eq!(trains.len(), 1);
        assert_eq!(trains[0].number, 6441);
        assert!((trains[0].lon - 5.486703).abs() < 1e-4);
        assert!((trains[0].lat - 51.443425).abs() < 1e-4);
    }

    #[test]
    fn survives_chunked_feed() {
        let xml: &[u8] = br#"<root><a:TreinLocation xmlns:a="x">
            <a:TreinNummer>42</a:TreinNummer>
            <a:TreinMaterieelDelen>
              <a:Longitude>1.5</a:Longitude>
              <a:Latitude>2.5</a:Latitude>
            </a:TreinMaterieelDelen>
          </a:TreinLocation></root>"#;
        let mut p = Parser::new();
        let mut out = Vec::new();
        for chunk in xml.chunks(7) {
            p.feed(chunk, |t| out.push(t));
        }
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].number, 42);
        assert!((out[0].lon - 1.5).abs() < 1e-6);
        assert!((out[0].lat - 2.5).abs() < 1e-6);
    }

    #[test]
    fn skips_self_closing_and_unrelated() {
        let xml = br#"<a:TreinLocation xmlns:a="x">
            <a:TreinNummer>7</a:TreinNummer>
            <a:TreinMaterieelDelen>
              <a:GeneratieTijd/>
              <a:Bron>GNSS</a:Bron>
              <a:Longitude>3.0</a:Longitude>
              <a:Latitude>4.0</a:Latitude>
              <a:Elevation>1.2</a:Elevation>
            </a:TreinMaterieelDelen>
          </a:TreinLocation>"#;
        let trains = parse_all(xml);
        assert_eq!(trains.len(), 1);
        assert_eq!(trains[0].number, 7);
    }
}
