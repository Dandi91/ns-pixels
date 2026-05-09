//! Schema-specific extractor for the NDOV `NStreinpositiesInterface5` feed.
//!
//! Walks the XML token stream from [`xmlparser`] and pulls three values out of
//! each `<TreinLocation>` element:
//!
//!   * `<TreinNummer>` text
//!   * the *first* `<TreinMaterieelDelen>` child's `<Longitude>` and
//!     `<Latitude>` text
//!
//! Subsequent `TreinMaterieelDelen` siblings are skipped — all parts of one
//! train are roughly co-located.

use xmlparser::{ElementEnd, Token, Tokenizer};

#[derive(Debug, Clone, Copy)]
pub struct Train {
    pub number: u32,
    pub lon: f32,
    pub lat: f32,
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Capture {
    None,
    Nummer,
    Lon,
    Lat,
}

pub fn parse<F: FnMut(Train)>(xml: &str, mut on_train: F) {
    let mut in_treinloc = false;
    let mut seen_first_part = false;
    let mut in_first_part = false;
    let mut capture = Capture::None;
    let mut current_local: &str = "";

    let mut nummer: Option<u32> = None;
    let mut lon: Option<f32> = None;
    let mut lat: Option<f32> = None;

    for tok in Tokenizer::from(xml).flatten() {
        match tok {
            Token::ElementStart { local, .. } => {
                let name = local.as_str();
                current_local = name;
                match name {
                    "TreinLocation" => {
                        in_treinloc = true;
                        seen_first_part = false;
                        in_first_part = false;
                        nummer = None;
                        lon = None;
                        lat = None;
                        capture = Capture::None;
                    }
                    "TreinNummer" if in_treinloc => capture = Capture::Nummer,
                    "TreinMaterieelDelen" if in_treinloc && !seen_first_part => {
                        in_first_part = true;
                        seen_first_part = true;
                    }
                    "Longitude" if in_first_part => capture = Capture::Lon,
                    "Latitude" if in_first_part => capture = Capture::Lat,
                    _ => {}
                }
            }
            Token::Text { text } => {
                let s = text.as_str().trim();
                match capture {
                    Capture::Nummer => nummer = s.parse().ok(),
                    Capture::Lon => lon = s.parse().ok(),
                    Capture::Lat => lat = s.parse().ok(),
                    Capture::None => {}
                }
            }
            Token::ElementEnd { end, .. } => {
                let name = match end {
                    ElementEnd::Close(_, local) => local.as_str(),
                    ElementEnd::Empty => current_local,
                    ElementEnd::Open => continue,
                };
                match name {
                    "TreinNummer" | "Longitude" | "Latitude" => {
                        capture = Capture::None;
                    }
                    "TreinMaterieelDelen" if in_first_part => {
                        in_first_part = false;
                    }
                    "TreinLocation" if in_treinloc => {
                        if let (Some(n), Some(lo), Some(la)) = (nummer, lon, lat) {
                            on_train(Train {
                                number: n,
                                lon: lo,
                                lat: la,
                            });
                        }
                        in_treinloc = false;
                        in_first_part = false;
                        seen_first_part = false;
                        capture = Capture::None;
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate alloc;
    use alloc::vec::Vec;

    fn parse_all(xml: &str) -> Vec<Train> {
        let mut out = Vec::new();
        parse(xml, |t| out.push(t));
        out
    }

    #[test]
    fn picks_first_part_only() {
        let xml = r#"<?xml version="1.0"?>
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
    fn skips_self_closing_and_unrelated() {
        let xml = r#"<a:TreinLocation xmlns:a="x">
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
