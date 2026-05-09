pub struct RdCoord {
    pub x: f32,
    pub y: f32,
}

pub fn wgs84_to_rd_fast(lat: f32, lon: f32) -> RdCoord {
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
    // Grouped by powers of d_lam
    let mut x = 155000.0;
    x += d_lam * (190094.945 - 11832.228 * d_phi - 114.221 * p2 - 2.340 * p3);
    x += l3 * (-32.391 - 0.608 * d_phi + 0.148 * p2);
    x += -0.705 * d_phi - 0.008 * p2;

    // Y (Northing) - Factored to minimize multiplications
    // Grouped by powers of d_phi and d_lam
    let mut y = 463000.0;
    y += d_phi * (309056.544 + 73.077 * d_phi + 59.788 * p2);
    y += l2 * (3638.893 - 157.984 * d_phi - 6.439 * p2);
    y += d_lam * (0.433 - 0.032 * d_phi);
    y += (l2 * l2) * (0.092 - 0.054 * d_phi); // l^4

    RdCoord { x, y }
}
