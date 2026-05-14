//! HUB75 64×64 LED matrix rendering pipeline.
//!
//! Mirrors the topology of `esp-hub75`'s `lcd_cam_bp.rs` example: two static
//! framebuffers ping-pong between a renderer and a DMA driver via a pair of
//! [`Signal`] channels. Both tasks run on the second core — `hub75_task` on a
//! high-priority interrupt executor (so DMA completion is serviced promptly)
//! and `display_task` on the standard executor.
//!
//! The render task currently draws a placeholder gradient; the train registry
//! is not wired in yet.

use core::cell::Cell;
use core::sync::atomic::{AtomicU32, Ordering};

use embassy_executor::Spawner;
use embassy_sync::{
    blocking_mutex::{Mutex as BlockingMutex, raw::CriticalSectionRawMutex},
    signal::Signal,
};
use embassy_time::{Duration, Instant};
use esp_hal::interrupt::software::SoftwareInterrupt;
use esp_hal::{
    gpio::AnyPin,
    interrupt::Priority,
    peripherals::{CPU_CTRL, DMA_CH0, LCD_CAM},
    system::Stack,
    time::Rate,
};
use esp_hub75::{
    Color, Hub75, Hub75Pins16,
    framebuffer::{bitplane::plain::DmaFrameBuffer, compute_rows},
};
use esp_println::println;
use esp_rtos::embassy::{Executor, InterruptExecutor};

use crate::registry::MAX_TRAINS;
use crate::train::{PixelData, ServiceType, TrainType};
use heapless::Vec;

pub const ROWS: usize = 64;
pub const COLS: usize = 64;
pub const NROWS: usize = compute_rows(ROWS);
pub const PLANES: usize = 7;

pub type FBType = DmaFrameBuffer<NROWS, COLS, PLANES>;
type FrameBufferExchange = Signal<CriticalSectionRawMutex, &'static mut FBType>;

const DISPLAY_STACK_SIZE: usize = 8192;
const FPS_SECONDS: u32 = 10;
const FPS_INTERVAL: Duration = Duration::from_secs(FPS_SECONDS as u64);

static REFRESH_RATE: AtomicU32 = AtomicU32::new(0);
static RENDER_RATE: AtomicU32 = AtomicU32::new(0);

/// All peripherals and pins consumed by the display pipeline. Constructed in
/// `main` from the `peripherals` bag and handed to [`start`].
pub struct DisplayPeripherals<'d> {
    pub lcd_cam: LCD_CAM<'d>,
    pub dma_channel: DMA_CH0<'d>,
    pub red1: AnyPin<'d>,
    pub grn1: AnyPin<'d>,
    pub blu1: AnyPin<'d>,
    pub red2: AnyPin<'d>,
    pub grn2: AnyPin<'d>,
    pub blu2: AnyPin<'d>,
    pub addr0: AnyPin<'d>,
    pub addr1: AnyPin<'d>,
    pub addr2: AnyPin<'d>,
    pub addr3: AnyPin<'d>,
    pub addr4: AnyPin<'d>,
    pub blank: AnyPin<'d>,
    pub clock: AnyPin<'d>,
    pub latch: AnyPin<'d>,
}

struct Hub75Owned {
    lcd_cam: LCD_CAM<'static>,
    dma_channel: DMA_CH0<'static>,
    pins: Hub75Pins16<'static>,
}

macro_rules! mk_static {
    ($t:ty, $val:expr) => {{
        static CELL: static_cell::StaticCell<$t> = static_cell::StaticCell::new();
        CELL.uninit().write($val)
    }};
}

/// Bring up the rendering pipeline on the second core. Spawns one high-pri
/// `hub75_task` and one normal-pri `display_task`; this call returns once the
/// second core has been started.
pub fn start(
    peripherals: DisplayPeripherals<'static>,
    cpu_ctrl: CPU_CTRL<'static>,
    sw_int_core: SoftwareInterrupt<'static, 1>,
    sw_int_hp: SoftwareInterrupt<'static, 2>,
) {
    let fb0 = mk_static!(FBType, FBType::new());
    fb0.erase();
    let fb1 = mk_static!(FBType, FBType::new());
    fb1.erase();

    let clusters0 = mk_static!(ClusterVec, ClusterVec::new());
    let clusters1 = mk_static!(ClusterVec, ClusterVec::new());
    FREE_CLUSTERS.signal(clusters1);

    static TX: FrameBufferExchange = FrameBufferExchange::new();
    static RX: FrameBufferExchange = FrameBufferExchange::new();

    let owned = Hub75Owned {
        lcd_cam: peripherals.lcd_cam,
        dma_channel: peripherals.dma_channel,
        pins: Hub75Pins16 {
            red1: peripherals.red1,
            grn1: peripherals.grn1,
            blu1: peripherals.blu1,
            red2: peripherals.red2,
            grn2: peripherals.grn2,
            blu2: peripherals.blu2,
            addr0: peripherals.addr0,
            addr1: peripherals.addr1,
            addr2: peripherals.addr2,
            addr3: peripherals.addr3,
            addr4: peripherals.addr4,
            blank: peripherals.blank,
            clock: peripherals.clock,
            latch: peripherals.latch,
        },
    };

    let app_core_stack = mk_static!(Stack<DISPLAY_STACK_SIZE>, Stack::new());

    let cpu1 = move || {
        let hp_executor = mk_static!(InterruptExecutor<2>, InterruptExecutor::new(sw_int_hp));
        let hp_spawner = hp_executor.start(Priority::Priority3);
        hp_spawner.spawn(hub75_task(owned, &RX, &TX, fb1).unwrap());

        let lp_executor = mk_static!(Executor, Executor::new());
        lp_executor.run(|spawner: Spawner| {
            spawner.spawn(display_task(&TX, &RX, fb0, clusters0).unwrap());
        });
    };

    esp_rtos::start_second_core(cpu_ctrl, sw_int_core, app_core_stack, cpu1);
}

#[embassy_executor::task]
async fn display_task(
    rx: &'static FrameBufferExchange,
    tx: &'static FrameBufferExchange,
    mut fb: &'static mut FBType,
    mut clusters: &'static mut ClusterVec,
) {
    println!("display_task: starting!");
    let mut count = 0u32;
    let mut start = Instant::now();

    loop {
        if let Some(new) = FRESH_CLUSTERS.try_take() {
            let old = core::mem::replace(&mut clusters, new);
            FREE_CLUSTERS.signal(old);
        }
        fb.erase();
        draw_trains(fb, clusters);

        tx.signal(fb);
        fb = rx.wait().await;

        count += 1;
        if start.elapsed() > FPS_INTERVAL {
            RENDER_RATE.store(count, Ordering::Relaxed);
            // println!(
            //     "display: render {} fps, refresh {} Hz",
            //     count / FPS_SECONDS,
            //     REFRESH_RATE.load(Ordering::Relaxed) / FPS_SECONDS,
            // );
            count = 0;
            start = Instant::now();
        }
    }
}

#[embassy_executor::task]
async fn hub75_task(
    owned: Hub75Owned,
    rx: &'static FrameBufferExchange,
    tx: &'static FrameBufferExchange,
    fb: &'static mut FBType,
) {
    println!("hub75_task: starting!");
    let descriptors = esp_hub75::hub75_dma_descriptors!(FBType);

    let mut hub75 = Hub75::<esp_hal::Async>::new_async(
        owned.lcd_cam,
        owned.pins,
        owned.dma_channel,
        descriptors,
        Rate::from_mhz(20),
    )
    .expect("failed to create Hub75!");

    let mut count = 0u32;
    let mut start = Instant::now();
    let mut fb = fb;

    // Hand off our initial buffer for the first render and
    // take the renderer's buffer as our first DMA source.
    let new_fb = rx.wait().await;
    tx.signal(fb);
    fb = new_fb;

    loop {
        if rx.signaled() {
            let new_fb = rx.wait().await;
            tx.signal(fb);
            fb = new_fb;
        }

        let mut xfer = hub75.render(fb).map_err(|(e, _)| e).expect("failed to start render!");
        xfer.wait_for_done().await.expect("dma wait_for_done failed");
        let (result, new_hub75) = xfer.wait();
        hub75 = new_hub75;
        if let Err(e) = result {
            println!("hub75: transfer failed: {:?}", e);
            continue;
        }

        count += 1;
        if start.elapsed() > FPS_INTERVAL {
            REFRESH_RATE.store(count, Ordering::Relaxed);
            count = 0;
            start = Instant::now();
        }
    }
}

/// Visualization modes the renderer can be in. The active mode is selected
/// externally (button input) and read by [`draw_trains`] each frame.
/// Animation mode — how a pixel's colors change over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VizMode {
    /// Each multi-type pixel cycles through its own member colors locally.
    PerCluster,
    /// The whole map pulses through the global type cycle; pixels matching
    /// the active type render bright, others dim in their own color.
    GlobalPulse,
}

impl VizMode {
    pub fn next(self) -> Self {
        match self {
            VizMode::PerCluster => VizMode::GlobalPulse,
            VizMode::GlobalPulse => VizMode::PerCluster,
        }
    }
}

/// Color axis — which train property colors are mapped from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorMode {
    /// Color by [`TrainType`] (SNG/SLT/Flirt/…).
    ByType,
    /// Color by [`ServiceType`] (Sprinter/Intercity/IntercityDirect/…).
    ByService,
}

impl ColorMode {
    pub fn next(self) -> Self {
        match self {
            ColorMode::ByType => ColorMode::ByService,
            ColorMode::ByService => ColorMode::ByType,
        }
    }
}

/// Combined display configuration. UP cycles `viz`, DOWN cycles `color`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DisplayConfig {
    pub viz: VizMode,
    pub col: ColorMode,
}

impl DisplayConfig {
    pub const fn new() -> Self {
        Self {
            viz: VizMode::PerCluster,
            col: ColorMode::ByType,
        }
    }
}

static CONFIG: BlockingMutex<CriticalSectionRawMutex, Cell<DisplayConfig>> =
    BlockingMutex::new(Cell::new(DisplayConfig::new()));

pub fn config() -> DisplayConfig {
    CONFIG.lock(|c| c.get())
}

pub fn update_config(f: impl FnOnce(DisplayConfig) -> DisplayConfig) -> DisplayConfig {
    CONFIG.lock(|c| {
        let next = f(c.get());
        c.set(next);
        next
    })
}

/// The display-side cluster snapshot. Producers (feed / ns_api) rebuild into
/// a free buffer and publish it; the display task drains the latest one each
/// frame. Sized for the worst case of one entry per train in the registry.
pub type ClusterVec = Vec<PixelData, MAX_TRAINS>;

/// Buffers the producer has filled and is asking the display to start using.
static FRESH_CLUSTERS: Signal<CriticalSectionRawMutex, &'static mut ClusterVec> = Signal::new();
/// Buffers the display is done with and the producer can reuse.
static FREE_CLUSTERS: Signal<CriticalSectionRawMutex, &'static mut ClusterVec> = Signal::new();

/// Producer-side: claim a free buffer to rebuild into. Returns `None` if the
/// display hasn't recycled the previous snapshot yet — in that case, skip
/// publishing this round; the next update will catch up.
pub fn try_take_free_clusters() -> Option<&'static mut ClusterVec> {
    FREE_CLUSTERS.try_take()
}

/// Producer-side: hand a freshly-rebuilt snapshot to the display. Pairs with
/// [`try_take_free_clusters`].
pub fn publish_clusters(v: &'static mut ClusterVec) {
    FRESH_CLUSTERS.signal(v);
}

/// Dwell time on a single highlighted type before the cross-fade into the
/// next type begins. Total cycle length is `ACTIVE_TYPES.len() * SLOT_MS`.
const SLOT_MS: u64 = 1500;
/// Tail end of each slot spent fading into the next type.
const FADE_MS: u64 = 500;
/// Brightness divisor applied to pixels that don't match the active type.
const DIM_DIV: u16 = 6;

/// Bit masks the global pulse cycles through, per color axis. `Unknown` is
/// excluded — it stays flat-dim regardless of the active slot.
const ACTIVE_TYPE_BITS: &[u8] = &[
    TrainType::SNG_BIT,
    TrainType::SLT_BIT,
    TrainType::FLIRT_BIT,
    TrainType::ICM_BIT,
    TrainType::DDZ_BIT,
    TrainType::VIRM_BIT,
    TrainType::ICNG_BIT,
];
const ACTIVE_SERVICE_BITS: &[u8] = &[
    ServiceType::SPRINTER_BIT,
    ServiceType::INTERCITY_BIT,
    ServiceType::INTERCITY_DIRECT_BIT,
];

/// Color axis bound to a [`ColorMode`]: how to extract the relevant bitmask
/// from a [`PixelData`], the color lookup, and the global-pulse cycle.
struct Axis {
    extract: fn(&PixelData) -> u8,
    color_for: fn(u8) -> [u8; 3],
    active: &'static [u8],
}

fn axis_for(mode: ColorMode) -> Axis {
    match mode {
        ColorMode::ByType => Axis {
            extract: |p| p.types,
            color_for: color_for_type_bit,
            active: ACTIVE_TYPE_BITS,
        },
        ColorMode::ByService => Axis {
            extract: |p| p.services,
            color_for: color_for_service_bit,
            active: ACTIVE_SERVICE_BITS,
        },
    }
}

/// Plot the current snapshot, dispatching on the active [`DisplayConfig`].
/// The snapshot is the display's own buffer; no registry lock is taken.
fn draw_trains(fb: &mut FBType, clusters: &ClusterVec) {
    let cfg = config();
    let axis = axis_for(cfg.col);
    let now_ms = Instant::now().as_millis();
    match cfg.viz {
        VizMode::PerCluster => draw_per_cluster(fb, clusters.as_slice(), now_ms, &axis),
        VizMode::GlobalPulse => draw_global_pulse(fb, clusters.as_slice(), now_ms, &axis),
    }
}

/// Per-pixel cycle: each multi-bit pixel independently rotates through its
/// member colors with a brief cross-fade. Single-bit pixels render flat.
fn draw_per_cluster(fb: &mut FBType, pixels: &[PixelData], now_ms: u64, axis: &Axis) {
    use embedded_graphics::prelude::Point;
    for e in pixels {
        let bits = (axis.extract)(e);
        let rgb = cluster_color(bits, now_ms, axis);
        let p = Point::new((e.coord_key >> 8) as i32, (e.coord_key & 0xff) as i32);
        fb.set_pixel(p, Color::new(rgb[0], rgb[1], rgb[2]));
    }
}

/// Cycle through every set bit in `bits`, holding each for `SLOT_MS - FADE_MS`
/// then linearly cross-fading into the next over `FADE_MS`.
fn cluster_color(bits: u8, now_ms: u64, axis: &Axis) -> [u8; 3] {
    let n = bits.count_ones() as u64;
    match n {
        0 => [0, 0, 0],
        1 => (axis.color_for)(bits),
        _ => {
            let phase = now_ms % (n * SLOT_MS);
            let slot = (phase / SLOT_MS) as u32;
            let in_slot = phase % SLOT_MS;
            let a = (axis.color_for)(nth_set_bit(bits, slot));
            if in_slot + FADE_MS <= SLOT_MS {
                a
            } else {
                let b = (axis.color_for)(nth_set_bit(bits, (slot + 1) % n as u32));
                let t = ((in_slot + FADE_MS - SLOT_MS) * 255 / FADE_MS) as u8;
                blend(a, b, t)
            }
        }
    }
}

/// Isolate the `n`-th (zero-indexed) set bit of `mask` as a single-bit `u8`.
fn nth_set_bit(mask: u8, n: u32) -> u8 {
    let mut m = mask;
    for _ in 0..n {
        m &= m - 1;
    }
    m & m.wrapping_neg()
}

/// Global pulse: the whole map highlights one axis value at a time. Pixels
/// matching the active bit render bright; others fall back to a dim color of
/// their own lowest-set bit on the same axis.
fn draw_global_pulse(fb: &mut FBType, pixels: &[PixelData], now_ms: u64, axis: &Axis) {
    use embedded_graphics::prelude::Point;
    let n = axis.active.len() as u64;
    let phase = now_ms % (n * SLOT_MS);
    let slot = (phase / SLOT_MS) as usize;
    let in_slot = phase % SLOT_MS;
    let active_a = axis.active[slot];
    let fade_t = if in_slot + FADE_MS <= SLOT_MS {
        None
    } else {
        let active_b = axis.active[(slot + 1) % axis.active.len()];
        let t = ((in_slot + FADE_MS - SLOT_MS) * 255 / FADE_MS) as u8;
        Some((active_b, t))
    };

    for e in pixels {
        let bits = (axis.extract)(e);
        let a = pixel_color(bits, active_a, axis);
        let rgb = match fade_t {
            None => a,
            Some((active_b, t)) => blend(a, pixel_color(bits, active_b, axis), t),
        };
        let p = Point::new((e.coord_key >> 8) as i32, (e.coord_key & 0xff) as i32);
        fb.set_pixel(p, Color::new(rgb[0], rgb[1], rgb[2]));
    }
}

/// Pick a pixel's color for one frame of the global pulse: full-bright active
/// color when the pixel contains the active bit, otherwise the dim color of
/// its lowest-set bit on the same axis.
fn pixel_color(bits: u8, active: u8, axis: &Axis) -> [u8; 3] {
    if bits == 0 {
        return [0, 0, 0];
    }
    if bits & active != 0 {
        return (axis.color_for)(active);
    }
    let fallback = bits & bits.wrapping_neg();
    dim((axis.color_for)(fallback))
}

fn dim(c: [u8; 3]) -> [u8; 3] {
    [
        (c[0] as u16 / DIM_DIV) as u8,
        (c[1] as u16 / DIM_DIV) as u8,
        (c[2] as u16 / DIM_DIV) as u8,
    ]
}

fn blend(a: [u8; 3], b: [u8; 3], t: u8) -> [u8; 3] {
    let lerp = |x: u8, y: u8| -> u8 {
        let inv = 255 - t as u16;
        let v = x as u16 * inv + y as u16 * t as u16;
        (v / 255) as u8
    };
    [lerp(a[0], b[0]), lerp(a[1], b[1]), lerp(a[2], b[2])]
}

/// Map a single-bit `TrainType` mask to its display color.
fn color_for_type_bit(bit: u8) -> [u8; 3] {
    match bit {
        TrainType::UNKNOWN_BIT => [10, 10, 10], // dim gray
        TrainType::SNG_BIT => [255, 80, 0],     // orange
        TrainType::SLT_BIT => [0, 200, 255],    // cyan
        TrainType::FLIRT_BIT => [255, 0, 200],  // magenta
        TrainType::ICM_BIT => [255, 220, 0],    // yellow
        TrainType::DDZ_BIT => [0, 255, 80],     // green
        TrainType::VIRM_BIT => [80, 80, 255],   // blue
        TrainType::ICNG_BIT => [255, 255, 255],
        _ => [0, 0, 0],
    }
}

/// Map a single-bit `ServiceType` mask to its display color.
fn color_for_service_bit(bit: u8) -> [u8; 3] {
    match bit {
        ServiceType::UNKNOWN_BIT => [10, 10, 10],           // dim gray
        ServiceType::SPRINTER_BIT => [0, 200, 255],         // cyan
        ServiceType::INTERCITY_BIT => [255, 220, 0],        // yellow
        ServiceType::INTERCITY_DIRECT_BIT => [255, 40, 40], // warm red
        _ => [0, 0, 0],
    }
}
