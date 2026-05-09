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

pub const ROWS: usize = 64;
pub const COLS: usize = 64;
pub const NROWS: usize = compute_rows(ROWS);
pub const PLANES: usize = 7;

pub type FBType = DmaFrameBuffer<NROWS, COLS, PLANES>;
type FrameBufferExchange = Signal<CriticalSectionRawMutex, &'static mut FBType>;

const DISPLAY_STACK_SIZE: usize = 8192;

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
            spawner.spawn(display_task(&TX, &RX, fb0).unwrap());
        });
    };

    esp_rtos::start_second_core(cpu_ctrl, sw_int_core, app_core_stack, cpu1);
}

#[embassy_executor::task]
async fn display_task(
    rx: &'static FrameBufferExchange,
    tx: &'static FrameBufferExchange,
    mut fb: &'static mut FBType,
) {
    println!("display_task: starting!");
    let mut frame: u32 = 0;
    let mut count = 0u32;
    let mut start = Instant::now();

    loop {
        fb.erase();
        draw_placeholder(fb, frame);
        frame = frame.wrapping_add(1);

        tx.signal(fb);
        fb = rx.wait().await;

        count += 1;
        if start.elapsed() > Duration::from_secs(1) {
            RENDER_RATE.store(count, Ordering::Relaxed);
            println!(
                "display: render {} fps, refresh {} Hz",
                count,
                REFRESH_RATE.load(Ordering::Relaxed)
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
        Rate::from_mhz(20),
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
        if start.elapsed() > Duration::from_secs(1) {
            REFRESH_RATE.store(count, Ordering::Relaxed);
            count = 0;
            start = Instant::now();
        }
    }
}

/// Placeholder pattern: animated RGB gradient bands. Replaced once the
/// registry is wired in.
fn draw_placeholder(fb: &mut FBType, frame: u32) {
    use embedded_graphics::prelude::*;
    const STEP: u8 = (256 / COLS) as u8;
    let phase = (frame & 0x3F) as i32;
    let bar_h = (ROWS as i32) / 3;
    for x in 0..COLS as i32 {
        let bright = (x as u8).wrapping_add(phase as u8).wrapping_mul(STEP);
        for y in 0..bar_h {
            fb.set_pixel(Point::new(x, y), Color::new(bright, 0, 0));
            fb.set_pixel(Point::new(x, y + bar_h), Color::new(0, bright, 0));
            fb.set_pixel(Point::new(x, y + 2 * bar_h), Color::new(0, 0, bright));
        }
    }
}
