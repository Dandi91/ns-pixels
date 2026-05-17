//! Map-mode state, canvas→display projection, and the controller task.
//!
//! `MapMode` selects which sub-region of the 224×224 canvas is rendered into
//! the 64×64 display. The controller task auto-cycles every
//! [`AUTO_INTERVAL`] and listens for manual flips on [`CHANGED`]; on either
//! event it rebuilds the snapshot immediately so the panel reflects the new
//! mode without waiting for the next feed/ns_api publish.

use core::sync::atomic::{AtomicU8, Ordering};

use embassy_futures::select::{Either, select};
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::signal::Signal;
use embassy_time::{Duration, Timer};

use crate::display;
use crate::projection::{DISPLAY_SIDE, PixelCoord};
use crate::registry::SharedRegistry;

/// Which sub-region of the canvas is being rendered. Independent of
/// [`crate::display::DisplayConfig`] and **not persisted** — auto-cycles
/// every [`AUTO_INTERVAL`] and can be flipped by a long UP press.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MapMode {
    /// Full Netherlands view — canvas is downsampled 3.5× into the 64×64 display.
    Netherlands = 0,
    /// 80 km × 80 km zoom on the Randstad — anchored west=78000, north=496000
    /// in RD, giving (62, 73) as the canvas-pixel offset of the display origin.
    Randstad = 1,
}

impl MapMode {
    pub fn next(self) -> Self {
        match self {
            MapMode::Netherlands => MapMode::Randstad,
            MapMode::Randstad => MapMode::Netherlands,
        }
    }
}

/// Top-left of the Randstad window in canvas pixels (`west_x / 1250`,
/// `(587500 - north_y) / 1250`).
const RANDSTAD_OFFSET_X: u8 = 62;
const RANDSTAD_OFFSET_Y: u8 = 73;

/// How often the renderer flips to the other map mode if no manual change
/// happens. Matches the user-facing "two views, ~5 min each" cadence.
pub const AUTO_INTERVAL: Duration = Duration::from_secs(5 * 60);

static MODE: AtomicU8 = AtomicU8::new(MapMode::Netherlands as u8);

/// Wakes the controller task whenever the mode has been changed manually so
/// the next snapshot is rebuilt immediately (rather than waiting for the
/// next feed/ns_api publish).
pub static CHANGED: Signal<CriticalSectionRawMutex, ()> = Signal::new();

pub fn current() -> MapMode {
    match MODE.load(Ordering::Relaxed) {
        0 => MapMode::Netherlands,
        _ => MapMode::Randstad,
    }
}

pub fn set(mode: MapMode) {
    MODE.store(mode as u8, Ordering::Relaxed);
}

/// Toggle the mode and signal the rebuild task. Use from manual triggers
/// (long-press, etc.); the auto-cycle task flips directly without signalling.
pub fn toggle() -> MapMode {
    let next = current().next();
    set(next);
    CHANGED.signal(());
    next
}

/// Project a canvas-pixel coordinate to display-pixel space for `mode`.
/// Returns `None` if the canvas coord is off-canvas or falls outside the
/// mode's display window. The 3.5× downsample for Netherlands mode uses
/// `canvas * 2 / 7` (the compiler turns the constant divisor into a multiply).
pub fn canvas_to_display(coord: PixelCoord, mode: MapMode) -> Option<PixelCoord> {
    if !coord.is_on_canvas() {
        return None;
    }
    let (dx, dy) = match mode {
        MapMode::Netherlands => {
            // 224 -> 64 == /3.5 == *2/7. Stays well under u16 even at canvas edge.
            ((coord.x as u16 * 2 / 7) as u8, (coord.y as u16 * 2 / 7) as u8)
        }
        MapMode::Randstad => {
            let x = coord.x.checked_sub(RANDSTAD_OFFSET_X)?;
            let y = coord.y.checked_sub(RANDSTAD_OFFSET_Y)?;
            (x, y)
        }
    };
    if dx >= DISPLAY_SIDE || dy >= DISPLAY_SIDE {
        return None;
    }
    Some(PixelCoord { x: dx, y: dy })
}

#[embassy_executor::task]
pub async fn run(registry: &'static SharedRegistry) {
    log::info!("map_mode task started");
    loop {
        match select(Timer::after(AUTO_INTERVAL), CHANGED.wait()).await {
            // Auto-cycle reached its deadline; flip the mode ourselves.
            Either::First(_) => {
                let next = current().next();
                set(next);
                log::info!("map_mode: auto -> {:?}", next);
            }
            // A manual change already flipped the mode (and signalled us);
            // just consume the event and republish.
            Either::Second(_) => {
                log::info!("map_mode: manual -> {:?}", current());
            }
        }
        republish(registry).await;
    }
}

async fn republish(registry: &SharedRegistry) {
    let Some(buf) = display::try_take_free_clusters() else {
        // No free buffer right now — the next feed/ns_api publish will pick
        // up the new mode within ~30 s, so the screen catches up shortly.
        return;
    };
    {
        let reg = registry.lock().await;
        reg.rebuild_clusters_into(buf, current());
    }
    display::publish_clusters(buf);
}
