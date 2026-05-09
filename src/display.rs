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

use core::sync::atomic::{AtomicU32, Ordering};

use embassy_executor::Spawner;
use embassy_sync::{blocking_mutex::raw::CriticalSectionRawMutex, signal::Signal};
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

use crate::registry::SharedRegistry;
use crate::train::TrainType;

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
    registry: &'static SharedRegistry,
    cpu_ctrl: CPU_CTRL<'static>,
    sw_int_core: SoftwareInterrupt<'static, 1>,
    sw_int_hp: SoftwareInterrupt<'static, 2>,
) {
    let fb0 = mk_static!(FBType, FBType::new());
    fb0.erase();
    let fb1 = mk_static!(FBType, FBType::new());
    fb1.erase();

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
            spawner.spawn(display_task(registry, &TX, &RX, fb0).unwrap());
        });
    };

    esp_rtos::start_second_core(cpu_ctrl, sw_int_core, app_core_stack, cpu1);
}

#[embassy_executor::task]
async fn display_task(
    registry: &'static SharedRegistry,
    rx: &'static FrameBufferExchange,
    tx: &'static FrameBufferExchange,
    mut fb: &'static mut FBType,
) {
    println!("display_task: starting!");
    let mut count = 0u32;
    let mut start = Instant::now();

    loop {
        fb.erase();
        draw_trains(fb, registry).await;

        tx.signal(fb);
        fb = rx.wait().await;

        count += 1;
        if start.elapsed() > FPS_INTERVAL {
            RENDER_RATE.store(count, Ordering::Relaxed);
            println!(
                "display: render {} fps, refresh {} Hz",
                count / FPS_SECONDS,
                REFRESH_RATE.load(Ordering::Relaxed) / FPS_SECONDS,
            );
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
        Rate::from_mhz(16),
    )
    .expect("failed to create Hub75!");

    let mut count = 0u32;
    let mut start = Instant::now();
    let mut fb = fb;

    // Hand off our initial buffer for the first render and take the renderer's
    // buffer as our first DMA source.
    let new_fb = rx.wait().await;
    tx.signal(fb);
    fb = new_fb;

    loop {
        if rx.signaled() {
            let new_fb = rx.wait().await;
            tx.signal(fb);
            fb = new_fb;
        }

        let mut xfer = hub75
            .render(fb)
            .map_err(|(e, _)| e)
            .expect("failed to start render!");
        xfer.wait_for_done()
            .await
            .expect("dma wait_for_done failed");
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

/// Per-train dwell on its color before the cross-fade into the next cluster
/// member begins. Total cycle length for a cluster of N is `N * SLOT_MS`.
const SLOT_MS: u64 = 1500;
/// Tail end of each slot spent fading into the next color.
const FADE_MS: u64 = 500;

/// Plot every train in the registry. When multiple trains share a pixel,
/// cycle through their colors over time with a gradual cross-fade.
/// Holds the registry mutex only for the collection step.
async fn draw_trains(fb: &mut FBType, registry: &SharedRegistry) {
    use embedded_graphics::prelude::Point;

    // (packed x<<8 | y, type) — small enough to copy out under the lock so
    // we can release it before sorting / grouping / rendering.
    let mut entries: heapless::Vec<(u16, TrainType), { crate::registry::MAX_TRAINS }> =
        heapless::Vec::new();
    {
        let reg = registry.lock().await;
        for (_, state) in reg.iter() {
            if !state.pixel.is_on_screen() {
                continue;
            }
            let key = ((state.pixel.x as u16) << 8) | state.pixel.y as u16;
            let _ = entries.push((key, state.typ));
        }
    }
    entries.sort_unstable_by_key(|e| e.0);

    let now_ms = Instant::now().as_millis();

    let mut i = 0;
    while i < entries.len() {
        let key = entries[i].0;
        let mut j = i + 1;
        while j < entries.len() && entries[j].0 == key {
            j += 1;
        }
        let rgb = cluster_color(&entries[i..j], now_ms);
        let p = Point::new((key >> 8) as i32, (key & 0xff) as i32);
        fb.set_pixel(p, Color::new(rgb[0], rgb[1], rgb[2]));
        i = j;
    }
}

/// Pick the cluster's current color. Single-train clusters render flat;
/// larger clusters cycle through members, holding each for `SLOT_MS - FADE_MS`
/// then linearly cross-fading into the next over `FADE_MS`.
fn cluster_color(cluster: &[(u16, TrainType)], now_ms: u64) -> [u8; 3] {
    if cluster.len() == 1 {
        return color_for(cluster[0].1);
    }
    let n = cluster.len() as u64;
    let phase = now_ms % (n * SLOT_MS);
    let slot = (phase / SLOT_MS) as usize;
    let in_slot = phase % SLOT_MS;
    let a = color_for(cluster[slot].1);
    if in_slot + FADE_MS <= SLOT_MS {
        a
    } else {
        let b = color_for(cluster[(slot + 1) % cluster.len()].1);
        let t = ((in_slot + FADE_MS - SLOT_MS) * 255 / FADE_MS) as u8;
        blend(a, b, t)
    }
}

fn blend(a: [u8; 3], b: [u8; 3], t: u8) -> [u8; 3] {
    let lerp = |x: u8, y: u8| -> u8 {
        let inv = 255 - t as u16;
        let v = x as u16 * inv + y as u16 * t as u16;
        (v / 255) as u8
    };
    [lerp(a[0], b[0]), lerp(a[1], b[1]), lerp(a[2], b[2])]
}

fn color_for(typ: TrainType) -> [u8; 3] {
    match typ {
        TrainType::SNG => [255, 80, 0],    // orange
        TrainType::SLT => [0, 200, 255],   // cyan
        TrainType::Flirt => [255, 0, 200], // magenta
        TrainType::ICM => [255, 220, 0],   // yellow
        TrainType::DDZ => [0, 255, 80],    // green
        TrainType::VIRM => [80, 80, 255],  // blue
        TrainType::ICNG => [255, 255, 255],
        TrainType::Unknown => [40, 40, 40], // dim gray
    }
}
