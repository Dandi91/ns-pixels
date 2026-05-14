//! Live registry of trains seen on the ZMQ feed.
//!
//! Backed by [`heapless::FnvIndexMap`] in internal SRAM for cache-friendly
//! iteration during rendering. Capacity is fixed at compile time.

use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::mutex::Mutex;
use embassy_time::{Duration, Instant};
use heapless::{FnvIndexMap, Vec};

use crate::projection::PixelCoord;
use crate::train::{PixelData, TrainState, TrainType};

/// Must be a power of two. ~16 bytes/entry → 8 KiB at N=512.
pub const MAX_TRAINS: usize = 512;

/// Upper bound on trains evicted in a single pass.
const EVICT_BATCH: usize = 64;

#[derive(Default)]
pub struct Registry {
    map: FnvIndexMap<u32, TrainState, MAX_TRAINS>,
    clusterized: Vec<PixelData, MAX_TRAINS>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn get_clusterized(&self) -> &[PixelData] {
        &self.clusterized
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

    /// Record an enrichment result for `number`. Always stamps
    /// `last_enrichment_attempt` so unresolved trains (passed as
    /// [`TrainType::Unknown`]) sit on the cooldown.
    pub fn set_type(&mut self, number: u32, typ: TrainType, now: Instant) {
        if let Some(s) = self.map.get_mut(&number) {
            s.typ = typ;
            s.last_enrichment = Some(now);
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

    /// Number of entries whose [`TrainType`] is still [`TrainType::Unknown`]
    /// — i.e. awaiting (or persistently missing) enrichment.
    pub fn unknown_count(&self) -> usize {
        self.map
            .values()
            .filter(|v| v.typ == TrainType::Unknown)
            .count()
    }

    /// Fill `buf` with up to `N` train numbers whose [`TrainType`] is
    /// [`TrainType::Unknown`] and that haven't been attempted within
    /// `cooldown`. Replaces any previous contents of `buf`.
    pub fn pending_enrichment<const N: usize>(&self, buf: &mut Vec<u32, N>, cooldown: Duration) {
        buf.clear();
        let now = Instant::now();
        for (k, v) in self.map.iter() {
            if v.typ != TrainType::Unknown {
                continue;
            }
            if let Some(last) = v.last_enrichment {
                if now.duration_since(last) < cooldown {
                    continue;
                }
            }
            if buf.push(*k).is_err() {
                break;
            }
        }
    }

    pub fn rebuild_clusters(&mut self) {
        self.clusterized.clear();
        for state in self.map.values() {
            if !state.pixel.is_on_screen() {
                continue;
            }
            // SAFETY: there are at most MAX_TRAINS entries in the map, so all should fit
            unsafe { self.clusterized.push_unchecked(state.into()) };
        }
        self.clusterized.sort_unstable_by_key(|e| e.coord_key);
        // Collapse runs of entries sharing a pixel into a single entry by OR-ing their type bitmasks together.
        // After sort, equal coord_keys are adjacent.
        let mut write = 0;
        let mut read = 0;
        while read < self.clusterized.len() {
            if write != read {
                self.clusterized[write] = self.clusterized[read];
            }
            read += 1;
            while read < self.clusterized.len()
                && self.clusterized[read].coord_key == self.clusterized[write].coord_key
            {
                self.clusterized[write].types |= self.clusterized[read].types;
                read += 1;
            }
            write += 1;
        }
        self.clusterized.truncate(write);
    }
}

// `CriticalSectionRawMutex` so the rendering task on core 1 can safely lock
// the registry that the feed/ns_api tasks update on core 0.
pub type SharedRegistry = Mutex<CriticalSectionRawMutex, Registry>;
