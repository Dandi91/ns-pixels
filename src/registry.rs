//! Live registry of trains seen on the ZMQ feed.
//!
//! Backed by [`heapless::FnvIndexMap`] in internal SRAM for cache-friendly
//! iteration during rendering. Capacity is fixed at compile time.

use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::mutex::Mutex;
use embassy_time::{Duration, Instant};
use heapless::{FnvIndexMap, Vec};

use crate::projection::PixelCoord;
use crate::train::{PixelData, ServiceType, TrainState, TrainType};

/// Must be a power of two. ~16 bytes/entry → 8 KiB at N=512.
pub const MAX_TRAINS: usize = 512;

/// Upper bound on trains evicted in a single pass.
const EVICT_BATCH: usize = 64;

#[derive(Default)]
pub struct Registry {
    map: FnvIndexMap<u32, TrainState, MAX_TRAINS>,
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

    /// Insert or refresh `number`. Returns `true` if the entry is new (caller
    /// should enqueue an enrichment fetch).
    pub fn upsert(&mut self, number: u32, pixel: PixelCoord, now: Instant) -> bool {
        if let Some(state) = self.map.get_mut(&number) {
            // The stored attempt offsets are relative to `last_seen`. When
            // `last_seen` advances by Δs, grow every non-sentinel offset by
            // Δs so the *absolute* attempt times stay anchored.
            let delta_s = now
                .checked_duration_since(state.last_seen)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            state.last_type_attempt_ago_s = bump_attempt_ago(state.last_type_attempt_ago_s, delta_s);
            state.last_service_attempt_ago_s = bump_attempt_ago(state.last_service_attempt_ago_s, delta_s);
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
    /// `last_type_attempt_ago_s` so unresolved trains (passed as
    /// [`TrainType::Unknown`]) sit on the type-fetch cooldown.
    pub fn set_type(&mut self, number: u32, typ: TrainType, now: Instant) {
        if let Some(s) = self.map.get_mut(&number) {
            s.typ = typ;
            s.last_type_attempt_ago_s = attempt_offset_s(s.last_seen, now);
        } else {
            log::warn!("set_type: unknown train number {}", number);
        }
    }

    /// Record a service-category enrichment result for `number`. Stamps
    /// `last_service_attempt_ago_s` regardless of outcome — separate from
    /// the type cooldown so the two phases don't gate each other.
    pub fn set_service(&mut self, number: u32, service: ServiceType, now: Instant) {
        if let Some(s) = self.map.get_mut(&number) {
            s.service = service;
            s.last_service_attempt_ago_s = attempt_offset_s(s.last_seen, now);
        } else {
            log::warn!("set_service: unknown train number {}", number);
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
    pub fn unknown_count(&self) -> (usize, usize) {
        let t = self.map.values().filter(|v| v.typ == TrainType::Unknown).count();
        let s = self.map.values().filter(|v| v.service == ServiceType::Unknown).count();
        (t, s)
    }

    /// Fill `buf` with up to `N` train numbers currently marked unknown for
    /// the given axis. Useful for debug logging when the count is small.
    /// Replaces any previous contents of `buf`.
    pub fn unknown_numbers<const N: usize>(&self, buf: &mut Vec<u32, N>, axis: UnknownAxis) {
        buf.clear();
        for (k, v) in self.map.iter() {
            let is_unknown = match axis {
                UnknownAxis::Type => v.typ == TrainType::Unknown,
                UnknownAxis::Service => v.service == ServiceType::Unknown,
            };
            if is_unknown && buf.push(*k).is_err() {
                break;
            }
        }
    }

    /// Fill `buf` with up to `N` train numbers whose [`TrainType`] is
    /// [`TrainType::Unknown`] and whose last type-fetch attempt is older
    /// than `cooldown`. Replaces any previous contents of `buf`.
    pub fn pending_enrichment<const N: usize>(&self, buf: &mut Vec<u32, N>, cooldown: Duration) {
        self.collect_pending(
            buf,
            cooldown,
            |v| v.typ == TrainType::Unknown,
            |v| v.last_type_attempt_ago_s,
        );
    }

    /// Like [`pending_enrichment`], but for service-category enrichment:
    /// only trains whose [`TrainType`] is already resolved (skipping likely-
    /// invalid ritnummers), whose [`ServiceType`] is still
    /// [`ServiceType::Unknown`], and whose last service-fetch attempt is
    /// older than `cooldown`.
    pub fn pending_service_enrichment<const N: usize>(&self, buf: &mut Vec<u32, N>, cooldown: Duration) {
        self.collect_pending(
            buf,
            cooldown,
            |v| v.typ != TrainType::Unknown && v.service == ServiceType::Unknown,
            |v| v.last_service_attempt_ago_s,
        );
    }

    fn collect_pending<const N: usize>(
        &self,
        buf: &mut Vec<u32, N>,
        cooldown: Duration,
        accept: impl Fn(&TrainState) -> bool,
        attempt_ago_s: impl Fn(&TrainState) -> u16,
    ) {
        buf.clear();
        let now = Instant::now();
        let cooldown_s = cooldown.as_secs();
        for (k, v) in self.map.iter() {
            if !accept(v) {
                continue;
            }
            let ago_s = attempt_ago_s(v);
            if ago_s != TrainState::ATTEMPT_NEVER {
                // Time since attempt = (now - last_seen) + offset.
                let since_seen_s = now.duration_since(v.last_seen).as_secs();
                let since_attempt_s = since_seen_s + ago_s as u64;
                if since_attempt_s < cooldown_s {
                    continue;
                }
            }
            if buf.push(*k).is_err() {
                break;
            }
        }
    }

    /// Rebuild the on-screen cluster snapshot into `out`, replacing its
    /// contents. Walks every visible train, sorts by pixel coordinate, then
    /// collapses adjacent same-pixel entries by OR-ing their type bitmasks.
    /// Caller owns the buffer so the display task can read it without holding
    /// the registry lock.
    pub fn rebuild_clusters_into(&self, out: &mut Vec<PixelData, MAX_TRAINS>) {
        out.clear();
        for state in self.map.values() {
            if !state.pixel.is_on_screen() {
                continue;
            }
            // SAFETY: at most MAX_TRAINS entries in the map, so all fit.
            unsafe { out.push_unchecked(state.into()) };
        }
        out.sort_unstable_by_key(|e| e.coord_key);
        // Collapse runs of entries sharing a pixel by OR-ing their type
        // bitmasks. After the sort, equal coord_keys are adjacent.
        let mut write = 0;
        let mut read = 0;
        while read < out.len() {
            if write != read {
                out[write] = out[read];
            }
            read += 1;
            while read < out.len() && out[read].coord_key == out[write].coord_key {
                out[write].types |= out[read].types;
                out[write].services |= out[read].services;
                read += 1;
            }
            write += 1;
        }
        out.truncate(write);
    }
}

/// Offset (in seconds) relative to `last_seen` for stamping a `last_*_attempt`
/// field. Semantically `last_seen - attempt_time`: how far *before* last_seen
/// the attempt happened. Attempts are stamped at the current moment, so the
/// value is 0 in the typical case (attempt >= last_seen) and only > 0 if the
/// caller passes a `last_seen` in the future.
fn attempt_offset_s(last_seen: Instant, now: Instant) -> u16 {
    last_seen
        .checked_duration_since(now)
        .map(|d| d.as_secs().min(TrainState::ATTEMPT_NEVER as u64 - 1) as u16)
        .unwrap_or(0)
}

/// Grow an attempt-ago offset by the amount `last_seen` advanced. Preserves
/// the [`TrainState::ATTEMPT_NEVER`] sentinel; saturates non-sentinel values
/// at `ATTEMPT_NEVER - 1`.
fn bump_attempt_ago(ago_s: u16, delta_s: u64) -> u16 {
    if ago_s == TrainState::ATTEMPT_NEVER {
        return TrainState::ATTEMPT_NEVER;
    }
    (ago_s as u64 + delta_s).min(TrainState::ATTEMPT_NEVER as u64 - 1) as u16
}

/// Selects which "unknown" axis [`Registry::unknown_numbers`] reports.
#[derive(Debug, Clone, Copy)]
pub enum UnknownAxis {
    Type,
    Service,
}

// `CriticalSectionRawMutex` so the rendering task on core 1 can safely lock
// the registry that the feed/ns_api tasks update on core 0.
pub type SharedRegistry = Mutex<CriticalSectionRawMutex, Registry>;
