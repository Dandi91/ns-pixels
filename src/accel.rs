use core::sync::atomic::{AtomicU8, Ordering};
use embassy_time::{Duration, Timer};
use esp_hal::{Async, i2c::master::I2c};

const DEVICE_ADDRESS: u8 = 0b0011001;
const CTRL_REG1: u8 = 0x20;
const OUT_X_L: u8 = 0x28; // contiguous with OUT_X_H, OUT_Y_L, OUT_Y_H - all read in a single 4-byte swoop
const THRESHOLD: u16 = 4096;

/// Set address MSB to indicate an auto-advancing read of multiple bytes
const fn read_multiple(addr: u8) -> u8 {
    addr | 0x80
}

#[derive(Debug, Copy, Clone, Default, PartialEq)]
#[repr(u8)]
pub enum Direction {
    #[default]
    XUp = 1,
    XDown = 2,
    YUp = 3,
    YDown = 4,
}

impl From<u8> for Direction {
    fn from(value: u8) -> Self {
        match value {
            1 => Direction::XUp,
            2 => Direction::XDown,
            3 => Direction::YUp,
            4 => Direction::YDown,
            _ => Direction::default(),
        }
    }
}

static DIRECTION: AtomicU8 = AtomicU8::new(Direction::XUp as u8);

pub fn get_direction() -> Direction {
    DIRECTION.load(Ordering::Relaxed).into()
}

#[embassy_executor::task]
pub async fn run(mut i2c: I2c<'static, Async>) {
    // CTRL_REG1: 10 Hz data rate, low-power disabled, Z-axis disabled
    i2c.write_async(DEVICE_ADDRESS, &[CTRL_REG1, 0b00100011]).await.unwrap();

    let mut buf = [0u8; 4];
    loop {
        Timer::after(Duration::from_millis(100)).await;

        let result = i2c
            .write_read_async(DEVICE_ADDRESS, &[read_multiple(OUT_X_L)], &mut buf)
            .await;
        if let Err(e) = result {
            log::error!("Error reading from I2C: {}", e);
            continue;
        }

        let x = i16::from_le_bytes(buf[0..=1].try_into().unwrap());
        let y = i16::from_le_bytes(buf[2..=3].try_into().unwrap());

        // Default chip settings produce values in approx range -2^14..2^14
        // Treat magnitudes lower than 2^12 as non-indicative and don't change the direction
        if x.unsigned_abs() < THRESHOLD && y.unsigned_abs() < THRESHOLD {
            continue;
        }

        let direction = if x.unsigned_abs() > y.unsigned_abs() {
            if x.is_positive() {
                Direction::XUp
            } else {
                Direction::XDown
            }
        } else {
            if y.is_positive() {
                Direction::YUp
            } else {
                Direction::YDown
            }
        };

        let previous: Direction = DIRECTION.swap(direction as u8, Ordering::Relaxed).into();
        if previous != direction {
            log::info!("Rotated, now direction={:?}", direction);
        }
    }
}
