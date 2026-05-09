use embassy_time::Instant;

use crate::projection::PixelCoord;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceType {
    Unknown,
    Sprinter,
    Intercity,
    IntercityDirect,
}

/// One row of the live registry. Identified externally by train number.
#[derive(Debug, Clone, Copy)]
pub struct TrainState {
    pub pixel: PixelCoord,
    pub last_seen: Instant,
    pub typ: TrainType,
    pub service: ServiceType,
}

impl TrainState {
    pub fn new(pixel: PixelCoord, last_seen: Instant) -> Self {
        Self {
            pixel,
            last_seen,
            typ: TrainType::Unknown,
            service: ServiceType::Unknown,
        }
    }
}
