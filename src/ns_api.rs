//! NS Virtual Train API enrichment task.
//!
//! Drains the new-train channel, batches up to [`BATCH_MAX`] IDs with a short
//! coalescing window, runs an HTTPS GET against the NS API, and applies the
//! returned [`TrainType`] back into the registry.

use core::fmt::Write as _;

use embassy_futures::select::{Either, select};
use embassy_net::Stack;
use embassy_net::dns::DnsSocket;
use embassy_net::tcp::client::{TcpClient, TcpClientState};
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::Channel;
use embassy_time::{Duration, Instant, Timer};
use heapless::{String, Vec};
use reqwless::{
    client::{HttpClient, TlsConfig, TlsVerify},
    request::{Method, RequestBuilder},
};
use serde::Deserialize;

use crate::registry::SharedRegistry;
use crate::train::TrainType;

pub const QUEUE_CAPACITY: usize = 64;
pub const BATCH_MAX: usize = 10;

/// Brief window after a wake-up to let the registry settle before sweeping.
const COALESCE: Duration = Duration::from_millis(500);
/// Fallback wake even if no new-train notifications arrive — picks up trains
/// dropped from the queue under load and retries failed/missing entries.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);
/// Pause between API calls if a previous call failed; avoids hammering the
/// gateway when something is broken.
const FAILURE_BACKOFF: Duration = Duration::from_secs(10);

const HOST: &str = "gateway.apiportal.ns.nl";
const API_KEY: &str = env!("NS_API_KEY");

// TLS record buffers (~17 KiB each; required for max TLS fragment).
const TLS_BUF_LEN: usize = 17 * 1024;
// HTTP response buffer. Each train is ~150 bytes of JSON; 16 KiB comfortably
// covers BATCH_MAX entries plus headers.
const HTTP_BUF_LEN: usize = 16 * 1024;

pub type NewTrainQueue = Channel<NoopRawMutex, u32, QUEUE_CAPACITY>;

/// Subset of the `getTrainInformation` response we care about. The endpoint
/// returns a JSON array; each element has many more fields (station, spoor,
/// materieeldelen, …) that serde-json-core ignores by default.
#[derive(Deserialize)]
struct TrainInfo<'a> {
    ritnummer: u32,
    #[serde(rename = "type", borrow)]
    typ: &'a str,
}

#[embassy_executor::task]
pub async fn run(
    stack: Stack<'static>,
    registry: &'static SharedRegistry,
    queue: &'static NewTrainQueue,
    tls_seed: u64,
) {
    log::info!("ns_api enrichment task started");

    // PSRAM-backed scratch space — TLS bufs are too large for internal SRAM.
    let mut tls_read = alloc::boxed::Box::<[u8], _>::new_uninit_slice_in(
        TLS_BUF_LEN,
        esp_alloc::ExternalMemory,
    );
    let mut tls_write = alloc::boxed::Box::<[u8], _>::new_uninit_slice_in(
        TLS_BUF_LEN,
        esp_alloc::ExternalMemory,
    );
    let mut http_buf = alloc::boxed::Box::<[u8], _>::new_uninit_slice_in(
        HTTP_BUF_LEN,
        esp_alloc::ExternalMemory,
    );
    // SAFETY: u8 has no validity requirements; consumers overwrite before reading.
    let tls_read = unsafe { tls_read.assume_init_mut() };
    let tls_write = unsafe { tls_write.assume_init_mut() };
    let http_buf = unsafe { http_buf.assume_init_mut() };

    let tcp_state: TcpClientState<1, 4096, 4096> = TcpClientState::new();
    let tcp_client = TcpClient::new(stack, &tcp_state);
    let dns = DnsSocket::new(stack);
    let mut rng_seed = tls_seed;

    let mut batch: Vec<u32, BATCH_MAX> = Vec::new();

    loop {
        // Wake on either a new-train notification (low-latency path) or the
        // periodic sweep (covers IDs dropped from a full channel and retries
        // failed lookups). The channel value itself is unused — registry is
        // the source of truth for what needs enriching.
        let _ = select(queue.receive(), Timer::after(SWEEP_INTERVAL)).await;
        // Drain any other queued wake-hints so they don't cause spurious
        // immediate re-runs after this iteration.
        while queue.try_receive().is_ok() {}

        // Brief settle window so a burst of notifications coalesces.
        Timer::after(COALESCE).await;

        // Drain the registry's Unknown trains in batches of BATCH_MAX,
        // continuing until the registry has none left or a fetch fails.
        loop {
            {
                let reg = registry.lock().await;
                reg.pending_enrichment(&mut batch);
            }
            if batch.is_empty() {
                break;
            }

            // Re-seed each call so a TLS session leak doesn't reuse RNG state.
            rng_seed = rng_seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let tls = TlsConfig::new(rng_seed, tls_read, tls_write, TlsVerify::None);
            let mut http = HttpClient::new_with_tls(&tcp_client, &dns, tls);

            match fetch_and_apply(&mut http, http_buf, &batch, registry).await {
                Ok(applied) => {
                    log::info!("ns_api: fetched {}/{} train infos", applied, batch.len());
                }
                Err(e) => {
                    log::warn!("ns_api: fetch failed: {:?} (batch={:?})", e, batch.as_slice());
                    Timer::after(FAILURE_BACKOFF).await;
                    break;
                }
            }
        }
    }
}

#[derive(Debug)]
enum FetchError {
    Http(reqwless::Error),
    HttpStatus(u16),
    InvalidUtf8,
    Json,
}

async fn fetch_and_apply<'a, T, D>(
    http: &mut HttpClient<'a, T, D>,
    http_buf: &mut [u8],
    batch: &[u32],
    registry: &SharedRegistry,
) -> Result<usize, FetchError>
where
    T: embedded_nal_async::TcpConnect + 'a,
    D: embedded_nal_async::Dns + 'a,
{
    // Build URL: https://<host>/virtual-train-api/v1/trein/<comma-separated-ids>
    // Max length: scheme+host (~50) + base (28) + BATCH_MAX*7 + 9 commas ≈ 160.
    let mut url: String<256> = String::new();
    let _ = write!(url, "https://{HOST}/virtual-train-api/v1/trein?ids=");
    for (i, id) in batch.iter().enumerate() {
        if i > 0 {
            let _ = url.push(',');
        }
        let _ = write!(url, "{}", id);
    }

    let headers = [
        ("Ocp-Apim-Subscription-Key", API_KEY),
        ("Accept", "application/json"),
    ];
    let req = http
        .request(Method::GET, &url)
        .await
        .map_err(FetchError::Http)?;
    let mut req = req.headers(&headers);
    let resp = req
        .send(http_buf)
        .await
        .map_err(FetchError::Http)?;

    let status = resp.status;
    if !status.is_successful() {
        return Err(FetchError::HttpStatus(status.0));
    }

    let body = resp
        .body()
        .read_to_end()
        .await
        .map_err(FetchError::Http)?;
    let body_str = core::str::from_utf8(body).map_err(|_| FetchError::InvalidUtf8)?;

    // Response is a JSON array of TrainInfo objects.
    let (parsed, _): (Vec<TrainInfo, BATCH_MAX>, usize) =
        serde_json_core::from_str(body_str).map_err(|_| FetchError::Json)?;

    let mut applied = 0;
    {
        let mut reg = registry.lock().await;
        for info in &parsed {
            reg.set_type(info.ritnummer, map_train_type(info.typ));
            applied += 1;
        }
    }
    Ok(applied)
}

fn map_train_type(s: &str) -> TrainType {
    // NS returns mixed-case strings like "Flirt", "VIRM-VI", "ICM-III".
    // Strip subtype suffix and normalise to uppercase before matching.
    let head = s.trim().split(['-', ' ']).next().unwrap_or(s);
    let mut buf = [0u8; 16];
    let n = head.len().min(buf.len());
    buf[..n].copy_from_slice(&head.as_bytes()[..n]);
    buf[..n].make_ascii_uppercase();
    match &buf[..n] {
        b"SNG" => TrainType::SNG,
        b"SLT" => TrainType::SLT,
        b"FLIRT" => TrainType::Flirt,
        b"ICM" => TrainType::ICM,
        b"DDZ" | b"DDAR" => TrainType::DDZ,
        b"VIRM" => TrainType::VIRM,
        b"ICNG" => TrainType::ICNG,
        _ => {
            log::info!("ns_api: unknown train type string {:?}", s);
            TrainType::Unknown
        }
    }
}
