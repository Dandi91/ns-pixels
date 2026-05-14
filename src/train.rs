use embassy_time::Instant;

use crate::projection::PixelCoord;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TrainType {
    Unknown,
    SNG,
    SLT,
    Flirt,
    ICM,
    DDZ,
    VIRM,
    ICNG,
}

impl TrainType {
    /// Single-bit mask representation; eight distinct values fit in a `u8` so
    /// clusters can be reduced to one byte covering every type present.
    pub const fn as_bit(self) -> u8 {
        match self {
            TrainType::Unknown => 1 << 0,
            TrainType::SNG => 1 << 1,
            TrainType::SLT => 1 << 2,
            TrainType::Flirt => 1 << 3,
            TrainType::ICM => 1 << 4,
            TrainType::DDZ => 1 << 5,
            TrainType::VIRM => 1 << 6,
            TrainType::ICNG => 1 << 7,
        }
    }

    pub const UNKNOWN_BIT: u8 = Self::Unknown.as_bit();
    pub const SNG_BIT: u8 = Self::SNG.as_bit();
    pub const SLT_BIT: u8 = Self::SLT.as_bit();
    pub const FLIRT_BIT: u8 = Self::Flirt.as_bit();
    pub const ICM_BIT: u8 = Self::ICM.as_bit();
    pub const DDZ_BIT: u8 = Self::DDZ.as_bit();
    pub const VIRM_BIT: u8 = Self::VIRM.as_bit();
    pub const ICNG_BIT: u8 = Self::ICNG.as_bit();
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ServiceType {
    Unknown,
    Sprinter,
    Intercity,
    IntercityDirect,
}

/// One row of the live registry. Identified externally by train number.
#[derive(Debug)]
pub struct TrainState {
    pub pixel: PixelCoord,
    pub last_seen: Instant,
    pub typ: TrainType,
    pub service: ServiceType,
    pub last_enrichment: Option<Instant>,
}

impl TrainState {
    pub fn new(pixel: PixelCoord, last_seen: Instant) -> Self {
        Self {
            pixel,
            last_seen,
            typ: TrainType::Unknown,
            service: ServiceType::Unknown,
            last_enrichment: None,
        }
    }
}

/// One on-screen pixel after cluster collapse. `types` is a bitmask of every
/// distinct [`TrainType`] sharing this pixel (see [`TrainType::as_bit`]).
#[derive(Debug, Clone, Copy)]
pub struct PixelData {
    pub coord_key: u16,
    pub types: u8,
}

impl From<&TrainState> for PixelData {
    fn from(state: &TrainState) -> Self {
        Self {
            coord_key: state.pixel.as_u16(),
            types: state.typ.as_bit(),
        }
    }
}
