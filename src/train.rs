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


pub enum ServiceType {
    Unknown,
    Sprinter,
    Intercity,
    IntercityDirect,
}

pub struct Train {
    pub number: u32,
    pub x: u8,
    pub y: u8,
    pub typ: TrainType,
    pub service: ServiceType,
}
