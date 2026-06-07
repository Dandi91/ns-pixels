//! Geographic projection.
//!
//! Trains are stored as **canvas-pixel** coordinates: a single 224×224 grid at
//! 1250 m/pixel covering the same NL bounding box as before. The display is
//! 64×64; each map mode projects from canvas pixels to display pixels with a
//! simple linear transform — see [`crate::map_mode::MapMode`] and the
//! per-mode helpers in that module.
//!
//! Keeping coords at canvas resolution lets us avoid re-running the WGS84→RD
//! transform on every snapshot rebuild while still supporting modes that
//! zoom into a sub-rectangle of the country.

/// Canvas-pixel coordinate (0..[`CANVAS_SIDE`]). Values at or above the
/// canvas size are off-canvas sentinels (e.g. trains way outside NL); filter
/// with [`PixelCoord::is_on_canvas`].
#[derive(Debug, Clone, Copy)]
pub struct PixelCoord {
    pub x: u8,
    pub y: u8,
}

impl PixelCoord {
    pub fn is_on_canvas(&self) -> bool {
        self.x < CANVAS_SIDE && self.y < CANVAS_SIDE
    }

    pub fn as_u16(&self) -> u16 {
        ((self.x as u16) << 8) | self.y as u16
    }
}

/// One side of the virtual canvas, in pixels. 280 km / 1250 m = 224.
pub const CANVAS_SIDE: u8 = 224;
/// Display dimensions; each map mode maps a sub-region of the canvas onto
/// this 64×64 grid.
pub const DISPLAY_SIDE: u8 = 64;

// RD bounding box covering the Netherlands; matches the Python reference.
const X_MIN: f32 = 0.0;
const Y_MIN: f32 = 307_500.0;
const M_PER_CANVAS_PIXEL: f32 = 1_250.0;
const CANVAS_SIDE_F: f32 = CANVAS_SIDE as f32;

/// Project a WGS-84 (lat, lon) pair onto the 224×224 canvas grid.
///
/// Internally computes RD-new coordinates (Amersfoort-centred), then scales
/// to canvas pixels with the same Y-flip as the display (north at the top).
/// Off-canvas results saturate above [`CANVAS_SIDE`]; filter with
/// [`PixelCoord::is_on_canvas`].
pub fn wgs84_to_canvas(lat: f32, lon: f32) -> PixelCoord {
    // Reference center (Amersfoort)
    const PHI0: f32 = 52.155174;
    const LAM0: f32 = 5.387206;

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
    x += d_lam * (190094.94 - 11832.228 * d_phi - 114.221 * p2 - 2.340 * p3);
    x += l3 * (-32.391 - 0.608 * d_phi + 0.148 * p2);
    x += -0.705 * d_phi - 0.008 * p2;

    // Y (Northing) - Factored to minimize multiplications
    let mut y = 463000.0;
    y += d_phi * (309056.53 + 73.077 * d_phi + 59.788 * p2);
    y += l2 * (3638.893 - 157.984 * d_phi - 6.439 * p2);
    y += d_lam * (0.433 - 0.032 * d_phi);
    y += (l2 * l2) * (0.092 - 0.054 * d_phi); // l^4

    // RD → canvas-pixel space; flip Y so south of NL is at the bottom.
    let px = (x - X_MIN) / M_PER_CANVAS_PIXEL;
    let py = (y - Y_MIN) / M_PER_CANVAS_PIXEL;
    let pixel_x = sat_u8(px);
    let pixel_y = sat_u8(CANVAS_SIDE_F - py);
    PixelCoord { x: pixel_x, y: pixel_y }
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
