/// Pixel coordinate on the 64×64 LED matrix. `x`/`y` may exceed `WIDTH`/
/// `HEIGHT` when the train is outside the displayed bounding box — callers
/// should filter with `is_on_screen`.
#[derive(Debug, Clone, Copy)]
pub struct PixelCoord {
    pub x: u8,
    pub y: u8,
}

impl PixelCoord {
    pub fn is_on_screen(&self) -> bool {
        self.x < WIDTH && self.y < HEIGHT
    }

    pub fn as_u16(&self) -> u16 {
        ((self.x as u16) << 8) | self.y as u16
    }
}

pub const WIDTH: u8 = 64;
pub const HEIGHT: u8 = 64;

// RD bounding box covering the Netherlands; matches the Python reference.
const X_MIN: f32 = 0.0;
const Y_MIN: f32 = 307_500.0;
const M_PER_PIXEL: f32 = 4_375.0; // 280 km / 64 px

/// Project a WGS-84 (lat, lon) pair onto the 64×64 LED matrix.
///
/// Internally computes RD-new coordinates (Amersfoort-centred), then maps
/// them to pixel space using the same bounding box and Y-flip as the Python
/// reference. The result is saturated to `u8`, so off-screen trains land on
/// the edges; use `PixelCoord::is_on_screen` to discard them.
pub fn wgs84_to_matrix(lat: f32, lon: f32) -> PixelCoord {
    // Reference center (Amersfoort)
    const PHI0: f32 = 52.15517440;
    const LAM0: f32 = 5.38720621;

    // Delta in "centidegrees"
    let d_phi = 0.36 * (lat - PHI0);
    let d_lam = 0.36 * (lon - LAM0);

    // Pre-calculate powers for reuse
    let p2 = d_phi * d_phi;
    let p3 = p2 * d_phi;
    let l2 = d_lam * d_lam;
    let l3 = l2 * d_lam;

    // X (Easting) - Factored to minimize multiplications
    let mut x = 155000.0;
    x += d_lam * (190094.945 - 11832.228 * d_phi - 114.221 * p2 - 2.340 * p3);
    x += l3 * (-32.391 - 0.608 * d_phi + 0.148 * p2);
    x += -0.705 * d_phi - 0.008 * p2;

    // Y (Northing) - Factored to minimize multiplications
    let mut y = 463000.0;
    y += d_phi * (309056.544 + 73.077 * d_phi + 59.788 * p2);
    y += l2 * (3638.893 - 157.984 * d_phi - 6.439 * p2);
    y += d_lam * (0.433 - 0.032 * d_phi);
    y += (l2 * l2) * (0.092 - 0.054 * d_phi); // l^4

    // RD → pixel space; flip Y so south of NL is at the bottom of the display.
    let px = (x - X_MIN) / M_PER_PIXEL;
    let py = (y - Y_MIN) / M_PER_PIXEL;
    let pixel_x = sat_u8(px);
    let pixel_y = sat_u8(HEIGHT as f32 - py);
    PixelCoord {
        x: pixel_x,
        y: pixel_y,
    }
}

fn sat_u8(v: f32) -> u8 {
    if v.is_nan() {
        return u8::MAX;
    }
    if v <= 0.0 {
        0
    } else if v >= 255.0 {
        255
    } else {
        v as u8
    }
}
