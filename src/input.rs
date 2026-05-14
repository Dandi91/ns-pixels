//! Button input task.
//!
//! Two momentary buttons (UP / DOWN) are wired to GPIO pins, normally open and
//! shorting to GND when pressed. Internal pull-ups hold the pin high at rest.
//! UP cycles through the available [`VizMode`]s; DOWN cycles through the
//! available [`ColorMode`]s.

use embassy_futures::select::{Either, select};
use embassy_time::{Duration, Timer};
use esp_hal::gpio::Input;
use esp_println::println;

use crate::display::{DisplayConfig, update_config};

/// Settling time after an edge before re-sampling. Inexpensive mechanical buttons
/// bounce for a few ms; 30 ms is comfortably past that without feeling laggy.
const DEBOUNCE: Duration = Duration::from_millis(30);

#[embassy_executor::task]
pub async fn run(mut up: Input<'static>, mut down: Input<'static>) {
    println!("input: button task started");
    loop {
        // Wait for whichever button fires first.
        let edge = select(up.wait_for_falling_edge(), down.wait_for_falling_edge()).await;

        // Debounce and confirm the pin is still pulled low — discards bounces
        // and stray edges.
        Timer::after(DEBOUNCE).await;
        match edge {
            Either::First(_) if up.is_low() => {
                let cfg = update_config(|c| DisplayConfig { viz: c.viz.next(), ..c });
                println!("input: UP -> viz {:?}", cfg.viz);
                up.wait_for_high().await;
            }
            Either::Second(_) if down.is_low() => {
                let cfg = update_config(|c| DisplayConfig {
                    color: c.color.next(),
                    ..c
                });
                println!("input: DOWN -> color {:?}", cfg.color);
                down.wait_for_high().await;
            }
            _ => {}
        }
    }
}
