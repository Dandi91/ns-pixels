//! Non-volatile [`DisplayConfig`] persistence.
//!
//! Reuses the default ESP-IDF NVS partition range (0x9000..0xF000, 24 KiB,
//! six 4 KiB sectors) as a private key-value store on top of
//! [`sequential_storage`]. Nothing else in this project touches that region.
//!
//! Flash writes are blocking (the CPU stalls for milliseconds while a sector
//! is erased/programmed), so saves are funneled through a dedicated task
//! that waits on [`SAVE_SIGNAL`] and processes one save at a time.

use embassy_embedded_hal::adapter::BlockingAsync;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::signal::Signal;
use esp_hal::peripherals::FLASH;
use esp_storage::FlashStorage;
use sequential_storage::{
    cache::NoCache,
    map::{MapConfig, MapStorage, SerializationError, Value},
};

use crate::display::{ColorMode, DisplayConfig, VizMode};

/// Default-partition NVS region. Aligned to the 4 KiB sector boundary as
/// required by `sequential-storage`.
const FLASH_RANGE: core::ops::Range<u32> = 0x9000..0xF000;

/// Single-byte key for the `DisplayConfig` entry. Pick distinct values per
/// stored item if more configs are added later.
const KEY_DISPLAY_CONFIG: u8 = 0;

/// Buffer for serialized key+value. `DisplayConfig` is 2 bytes today; 16 is
/// well past the flash-word alignment requirement and leaves headroom.
const SCRATCH_LEN: usize = 16;

type Flash = BlockingAsync<FlashStorage<'static>>;
type Map = MapStorage<u8, Flash, NoCache>;

/// Wakes the persist task whenever the in-memory config changes. The signal
/// carries the latest `DisplayConfig`; coalescing happens naturally since
/// `Signal` only keeps the most recent value.
pub static SAVE_SIGNAL: Signal<CriticalSectionRawMutex, DisplayConfig> = Signal::new();

/// Encoded as `[viz, col]`. Both fields are tiny enums; one byte each.
impl Value<'_> for DisplayConfig {
    fn serialize_into(&self, buffer: &mut [u8]) -> Result<usize, SerializationError> {
        if buffer.len() < 2 {
            return Err(SerializationError::BufferTooSmall);
        }
        buffer[0] = viz_to_u8(self.viz);
        buffer[1] = col_to_u8(self.col);
        Ok(2)
    }

    fn deserialize_from(buffer: &[u8]) -> Result<(Self, usize), SerializationError> {
        if buffer.len() < 2 {
            return Err(SerializationError::BufferTooSmall);
        }
        let viz = viz_from_u8(buffer[0]).ok_or(SerializationError::InvalidFormat)?;
        let col = col_from_u8(buffer[1]).ok_or(SerializationError::InvalidFormat)?;
        Ok((DisplayConfig::new(viz, col), 2))
    }
}

fn viz_to_u8(v: VizMode) -> u8 {
    match v {
        VizMode::PerCluster => 0,
        VizMode::GlobalPulse => 1,
    }
}

fn viz_from_u8(b: u8) -> Option<VizMode> {
    Some(match b {
        0 => VizMode::PerCluster,
        1 => VizMode::GlobalPulse,
        _ => return None,
    })
}

fn col_to_u8(c: ColorMode) -> u8 {
    match c {
        ColorMode::ByType => 0,
        ColorMode::ByService => 1,
    }
}

fn col_from_u8(b: u8) -> Option<ColorMode> {
    Some(match b {
        0 => ColorMode::ByType,
        1 => ColorMode::ByService,
        _ => return None,
    })
}

/// Wraps the flash-backed key-value store. Holds the only `FlashStorage` in
/// the program — flash access must be single-owner since the ROM routines
/// aren't reentrant.
pub struct ConfigStore {
    map: Map,
}

impl ConfigStore {
    pub fn new(flash: FLASH<'static>) -> Self {
        // ESP32-S3 is dual-core; the display task lives on the second core.
        // `multicore_auto_park` halts that core for the few-ms duration of
        // each flash write and restarts it afterward — without this, writes
        // fail with `OtherCoreRunning`. The LCD_CAM DMA keeps driving the
        // panel while the core is parked, so the display just holds the
        // current frame for the duration.
        let storage = BlockingAsync::new(FlashStorage::new(flash).multicore_auto_park());
        Self {
            map: MapStorage::new(storage, const { MapConfig::new(FLASH_RANGE) }, NoCache::new()),
        }
    }

    pub async fn load(&mut self) -> Option<DisplayConfig> {
        let mut buf = [0u8; SCRATCH_LEN];
        match self
            .map
            .fetch_item::<DisplayConfig>(&mut buf, &KEY_DISPLAY_CONFIG)
            .await
        {
            Ok(opt) => opt,
            Err(e) => {
                log::warn!("persist: load failed: {:?}", e);
                None
            }
        }
    }

    pub async fn save(&mut self, cfg: DisplayConfig) -> bool {
        let mut buf = [0u8; SCRATCH_LEN];
        match self.map.store_item(&mut buf, &KEY_DISPLAY_CONFIG, &cfg).await {
            Ok(()) => true,
            Err(e) => {
                log::warn!("persist: save failed: {:?}", e);
                false
            }
        }
    }
}

/// Signal the persist task to save `cfg` to flash. Returns immediately;
/// the write happens on the persist task.
pub fn request_save(cfg: DisplayConfig) {
    SAVE_SIGNAL.signal(cfg);
}

#[embassy_executor::task]
pub async fn run(mut store: ConfigStore) {
    log::info!("persist task started");
    loop {
        let cfg = SAVE_SIGNAL.wait().await;
        if store.save(cfg).await {
            log::info!("persist: saved {:?}", cfg);
        }
    }
}
