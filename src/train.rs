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

impl ServiceType {
    /// Single-bit mask representation; the four variants fit in the low nibble
    /// of a `u8` so clusters can carry a bitmask of every service type present.
    pub const fn as_bit(self) -> u8 {
        match self {
            ServiceType::Unknown => 1 << 0,
            ServiceType::Sprinter => 1 << 1,
            ServiceType::Intercity => 1 << 2,
            ServiceType::IntercityDirect => 1 << 3,
        }
    }

    pub const UNKNOWN_BIT: u8 = Self::Unknown.as_bit();
    pub const SPRINTER_BIT: u8 = Self::Sprinter.as_bit();
    pub const INTERCITY_BIT: u8 = Self::Intercity.as_bit();
    pub const INTERCITY_DIRECT_BIT: u8 = Self::IntercityDirect.as_bit();
}

/// One row of the live registry. Identified externally by train number.
#[derive(Debug)]
pub struct TrainState {
    pub pixel: PixelCoord,
    pub last_seen: Instant,
    pub typ: TrainType,
    pub service: ServiceType,
    /// Seconds between `last_seen` and the most recent train-type enrichment
    /// attempt, i.e. `last_seen - last_type_attempt`. Sentinel
    /// [`TrainState::ATTEMPT_NEVER`] means no attempt has been made yet.
    /// Trains live at most 5 minutes, so the value never exceeds 300.
    pub last_type_attempt_ago_s: u16,
    /// Same as [`last_type_attempt_ago_s`] but for the service-category
    /// enrichment phase. Tracked separately so a successful type fetch
    /// doesn't gate the immediately-following service fetch.
    pub last_service_attempt_ago_s: u16,
}

impl TrainState {
    /// Sentinel for `last_*_attempt_ago_s` meaning "no attempt yet".
    pub const ATTEMPT_NEVER: u16 = u16::MAX;

    pub fn new(pixel: PixelCoord, last_seen: Instant) -> Self {
        Self {
            pixel,
            last_seen,
            typ: TrainType::Unknown,
            service: ServiceType::Unknown,
            last_type_attempt_ago_s: Self::ATTEMPT_NEVER,
            last_service_attempt_ago_s: Self::ATTEMPT_NEVER,
        }
    }
}

/// One on-screen pixel after cluster collapse. `types` / `services` are
/// bitmasks of every distinct [`TrainType`] / [`ServiceType`] sharing the
/// pixel (see the respective `as_bit` methods).
#[derive(Debug, Clone, Copy)]
pub struct PixelData {
    pub coord_key: u16,
    pub types: u8,
    pub services: u8,
}
