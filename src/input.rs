//! Button input task.
//!
//! Two momentary buttons (UP / DOWN) are wired to GPIO pins, normally open and
//! shorting to GND when pressed. Internal pull-ups hold the pin high at rest.
//! UP: short press cycles [`VizMode`]; long press toggles
//! [`crate::map_mode::MapMode`]. DOWN: short press cycles [`ColorMode`].

use embassy_futures::select::{Either, select};
use embassy_time::{Duration, Timer};
use esp_hal::gpio::Input;
use esp_println::println;

use crate::display::{DisplayConfig, update_config};
use crate::map_mode;
use crate::persist;

/// Settling time after an edge before re-sampling. Inexpensive mechanical buttons
/// bounce for a few ms; 30 ms is comfortably past that without feeling laggy.
const DEBOUNCE: Duration = Duration::from_millis(30);
/// How long UP must be held to register as a long press (map-mode toggle).
const LONG_PRESS: Duration = Duration::from_millis(800);

#[embassy_executor::task]
pub async fn run(mut up: Input<'static>, mut down: Input<'static>) {
    println!("input: button task started");
    loop {
        // Wait for whichever button fires first.
        let edge = select(up.wait_for_falling_edge(), down.wait_for_falling_edge()).await;

        // Debounce and confirm the pin is still pulled low — discards bounces and stray edges.
        Timer::after(DEBOUNCE).await;
        match edge {
            Either::First(_) if up.is_low() => {
                // Race the release against the long-press threshold.
                match select(up.wait_for_high(), Timer::after(LONG_PRESS)).await {
                    Either::First(_) => {
                        // Released before the threshold — short press: cycle viz.
                        let cfg = update_config(|c| DisplayConfig::new(c.viz.next(), c.col));
                        println!("input: UP short -> viz {:?}", cfg.viz);
                        persist::request_save(cfg);
                    }
                    Either::Second(_) => {
                        // Held past the threshold — long press: toggle map mode.
                        // Not persisted; mode resets to default on reboot.
                        let next = map_mode::toggle();
                        println!("input: UP long -> map {:?}", next);
                        // Wait for release before listening for new edges.
                        up.wait_for_high().await;
                    }
                }
            }
            Either::Second(_) if down.is_low() => {
                let cfg = update_config(|c| DisplayConfig::new(c.viz, c.col.next()));
                println!("input: DOWN -> color {:?}", cfg.col);
                persist::request_save(cfg);
                down.wait_for_high().await;
            }
            _ => {}
        }
    }
}
