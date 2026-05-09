//! Live registry of trains seen on the ZMQ feed.
//!
//! Backed by [`heapless::FnvIndexMap`] in internal SRAM for cache-friendly
//! iteration during rendering. Capacity is fixed at compile time.

use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::mutex::Mutex;
use embassy_time::Instant;
use heapless::{FnvIndexMap, Vec};

use crate::projection::PixelCoord;
use crate::train::{TrainState, TrainType};

/// Must be a power of two. ~16 bytes/entry → 8 KiB at N=512.
pub const MAX_TRAINS: usize = 512;

/// Upper bound on trains evicted in a single pass.
const EVICT_BATCH: usize = 64;

pub struct Registry {
    map: FnvIndexMap<u32, TrainState, MAX_TRAINS>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub const fn new() -> Self {
        Self {
            map: FnvIndexMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Insert or refresh `number`. Returns `true` if the entry is new (caller
    /// should enqueue an enrichment fetch).
    pub fn upsert(&mut self, number: u32, pixel: PixelCoord, now: Instant) -> bool {
        if let Some(state) = self.map.get_mut(&number) {
            state.pixel = pixel;
            state.last_seen = now;
            false
        } else {
            // FnvIndexMap drops the insert silently on overflow; the train is
            // simply missing from the registry until capacity frees up.
            let _ = self.map.insert(number, TrainState::new(pixel, now));
            true
        }
    }

    pub fn set_type(&mut self, number: u32, typ: TrainType) {
        if let Some(s) = self.map.get_mut(&number) {
            s.typ = typ;
        }
    }

    /// Drop entries last seen before `cutoff`. Returns the eviction count.
    /// At most `EVICT_BATCH` per call to bound the time spent under lock.
    pub fn evict_older_than(&mut self, cutoff: Instant) -> usize {
        let mut to_remove: Vec<u32, EVICT_BATCH> = Vec::new();
        for (k, v) in self.map.iter() {
            if v.last_seen < cutoff {
                if to_remove.push(*k).is_err() {
                    break;
                }
            }
        }
        for k in &to_remove {
            self.map.remove(k);
        }
        to_remove.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&u32, &TrainState)> {
        self.map.iter()
    }

    /// Number of entries whose [`TrainType`] is still [`TrainType::Unknown`]
    /// — i.e. awaiting (or persistently missing) enrichment.
    pub fn unknown_count(&self) -> usize {
        self.map
            .values()
            .filter(|v| v.typ == TrainType::Unknown)
            .count()
    }

    /// Fill `buf` with up to `N` train numbers whose [`TrainType`] is
    /// [`TrainType::Unknown`] — i.e. the work-list for the enrichment task.
    /// Replaces any previous contents of `buf`.
    pub fn pending_enrichment<const N: usize>(&self, buf: &mut Vec<u32, N>) {
        buf.clear();
        for (k, v) in self.map.iter() {
            if v.typ == TrainType::Unknown && buf.push(*k).is_err() {
                break;
            }
        }
    }
}

pub type SharedRegistry = Mutex<NoopRawMutex, Registry>;
