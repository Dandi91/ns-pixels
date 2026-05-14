//! Button input task.
//!
//! Currently mocked: a single Embassy task that cycles [`VizMode`] on a fixed
//! interval so the rest of the pipeline can be exercised without hardware.
//! Replace [`run`]'s body with real GPIO polling / interrupt handling once
//! the button pins are wired in.

use embassy_time::{Duration, Timer};
use esp_println::println;

use crate::display::{set_viz_mode, viz_mode};

/// How often the mock "button" fires. Real hardware will replace this with
/// a debounced interrupt.
const MOCK_CYCLE: Duration = Duration::from_secs(60);

#[embassy_executor::task]
pub async fn run() {
    println!("input: mock button task started");
    loop {
        Timer::after(MOCK_CYCLE).await;
        let next = viz_mode().next();
        set_viz_mode(next);
        println!("input: viz mode -> {:?}", next);
    }
}
