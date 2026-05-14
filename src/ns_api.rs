//! NS API enrichment task.
//!
//! Drains the new-train channel and runs two complementary phases against
//! `gateway.apiportal.ns.nl`:
//!
//! - **Phase A — train type**: batched GET against
//!   `/virtual-train-api/v1/trein`, ~150 B/train, up to [`BATCH_MAX`] per call.
//! - **Phase B — service category**: one-train-per-request streaming scan of
//!   `/reisinformatie-api/api/v2/journey?train=N` for the first occurrence of
//!   `"categoryCode":"…"`. The full body is ~30 KB but the field lands inside
//!   the first ~1 KB, so we never buffer the tail.
//!
//! Both phases share the TLS/HTTP buffers and apply results to the same
//! [`SharedRegistry`], publishing a fresh cluster snapshot after every
//! successful call.

use core::fmt::Write as _;

use embassy_futures::select::select;
use embassy_net::Stack;
use embassy_net::dns::DnsSocket;
use embassy_net::tcp::client::{TcpClient, TcpClientState};
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::Channel;
use embassy_time::{Duration, Instant, Timer};
use embedded_io_async::Read;
use heapless::{String, Vec};
use reqwless::{
    client::{HttpClient, TlsConfig, TlsVerify},
    request::{Method, RequestBuilder},
};
use serde::Deserialize;

use crate::display;
use crate::leak_psram_slice;
use crate::registry::SharedRegistry;
use crate::train::{ServiceType, TrainType};

pub const QUEUE_CAPACITY: usize = 64;
pub const BATCH_MAX: usize = 10;
/// How many service-category lookups to issue per wake-up. Each is a single
/// HTTPS round-trip, so keep this small to avoid blocking type enrichment.
const SERVICE_BATCH_MAX: usize = 4;
/// Upper bound on bytes pulled from the journey response before giving up on
/// finding `categoryCode`. The field reliably appears within the first stop;
/// 4 KiB is plenty of headroom.
const SERVICE_SCAN_LIMIT: usize = 4 * 1024;

/// Brief window after a wake-up to let the registry settle before sweeping.
const COALESCE: Duration = Duration::from_millis(500);
/// Fallback wake even if no new-train notifications arrive — picks up trains
/// dropped from the queue under load and retries failed/missing entries.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);
/// Pause between API calls if a previous call failed; avoids hammering the
/// gateway when something is broken.
const FAILURE_BACKOFF: Duration = Duration::from_secs(10);
/// Minimum gap between enrichment attempts for the same train number. Trains
/// whose info the API never returns (or returns in an unparseable form) sit on
/// this cooldown instead of being re-requested every sweep.
const RETRY_COOLDOWN: Duration = Duration::from_secs(60);

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
    #[serde(rename = "ritnummer")]
    number: u32,
    #[serde(rename = "type", borrow, default)]
    train_type: Option<&'a str>,
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
    let tls_read = leak_psram_slice(TLS_BUF_LEN);
    let tls_write = leak_psram_slice(TLS_BUF_LEN);
    let http_buf = leak_psram_slice(HTTP_BUF_LEN);

    let tcp_state: TcpClientState<1, 4096, 4096> = TcpClientState::new();
    let tcp_client = TcpClient::new(stack, &tcp_state);
    let dns = DnsSocket::new(stack);
    let mut rng_seed = tls_seed;

    let mut type_batch: Vec<u32, BATCH_MAX> = Vec::new();
    let mut service_batch: Vec<u32, SERVICE_BATCH_MAX> = Vec::new();

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

        // Phase A: batched train-type fetch.
        let type_failed = loop {
            {
                let reg = registry.lock().await;
                reg.pending_enrichment(&mut type_batch, RETRY_COOLDOWN);
            }
            if type_batch.is_empty() {
                break false;
            }

            rng_seed = rng_seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let tls = TlsConfig::new(rng_seed, tls_read, tls_write, TlsVerify::None);
            let mut http = HttpClient::new_with_tls(&tcp_client, &dns, tls);

            match fetch_types(&mut http, http_buf, &type_batch, registry).await {
                Ok(applied) => {
                    log::info!("ns_api: type fetched {}/{}", applied, type_batch.len());
                    publish_snapshot(registry).await;
                }
                Err(e) => {
                    log::warn!("ns_api: type fetch failed: {:?} (batch={:?})", e, type_batch.as_slice());
                    break true;
                }
            }
        };

        if type_failed {
            Timer::after(FAILURE_BACKOFF).await;
            continue;
        }

        // Phase B: per-train service-category fetch via streaming scan.
        let service_failed = loop {
            {
                let reg = registry.lock().await;
                reg.pending_service_enrichment(&mut service_batch, RETRY_COOLDOWN);
            }
            if service_batch.is_empty() {
                break false;
            }

            let mut any_failure = false;
            // SERVICE_BATCH_MAX is small; doing each call sequentially keeps
            // the TLS buffers reusable. Each iteration creates a fresh client.
            for &number in service_batch.iter() {
                rng_seed = rng_seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let tls = TlsConfig::new(rng_seed, tls_read, tls_write, TlsVerify::None);
                let mut http = HttpClient::new_with_tls(&tcp_client, &dns, tls);

                match fetch_service(&mut http, http_buf, number, registry).await {
                    Ok(service) => {
                        log::info!("ns_api: service {} -> {:?}", number, service);
                    }
                    Err(e) => {
                        log::warn!("ns_api: service fetch {} failed: {:?}", number, e);
                        any_failure = true;
                        break;
                    }
                }
            }
            publish_snapshot(registry).await;
            if any_failure {
                break true;
            }
        };

        if service_failed {
            Timer::after(FAILURE_BACKOFF).await;
        }
    }
}

/// Rebuild the cluster snapshot and hand it to the display, if a free buffer
/// is available. Skipping when none is free is fine — the next refresh catches
/// up.
async fn publish_snapshot(registry: &SharedRegistry) {
    if let Some(buf) = display::try_take_free_clusters() {
        {
            let reg = registry.lock().await;
            reg.rebuild_clusters_into(buf);
        }
        display::publish_clusters(buf);
    }
}

#[allow(dead_code)]
#[derive(Debug)]
enum FetchError {
    Http(reqwless::Error),
    HttpStatus(u16),
    InvalidUtf8,
    Json,
    Stream,
}

async fn fetch_types<'a, T, D>(
    http: &mut HttpClient<'a, T, D>,
    http_buf: &mut [u8],
    batch: &[u32],
    registry: &SharedRegistry,
) -> Result<usize, FetchError>
where
    T: embedded_nal_async::TcpConnect + 'a,
    D: embedded_nal_async::Dns + 'a,
{
    // Build URL: https://<host>/virtual-train-api/v1/trein?ids=<comma-separated-ids>
    // Max length: scheme+host (~50) + base (32) + BATCH_MAX*7 + 9 commas ≈ 160.
    let mut url: String<256> = String::new();
    let _ = write!(url, "https://{HOST}/virtual-train-api/v1/trein?ids=");
    for (i, id) in batch.iter().enumerate() {
        if i > 0 {
            let _ = url.push(',');
        }
        let _ = write!(url, "{}", id);
    }

    let headers = [("Ocp-Apim-Subscription-Key", API_KEY), ("Accept", "application/json")];
    let req = http.request(Method::GET, &url).await.map_err(FetchError::Http)?;
    let mut req = req.headers(&headers);
    let resp = req.send(http_buf).await.map_err(FetchError::Http)?;

    let status = resp.status;
    if !status.is_successful() {
        return Err(FetchError::HttpStatus(status.0));
    }

    let body = resp.body().read_to_end().await.map_err(FetchError::Http)?;
    let body_str = core::str::from_utf8(body).map_err(|_| FetchError::InvalidUtf8)?;

    // Response is a JSON array of TrainInfo objects.
    let (parsed, _): (Vec<TrainInfo, { BATCH_MAX * 5 }>, usize) =
        serde_json_core::from_str(body_str).map_err(|_| FetchError::Json)?;

    let now = Instant::now();
    let mut applied = 0;
    {
        let mut reg = registry.lock().await;
        for info in &parsed {
            let typ = map_train_type(info.train_type);
            if typ != TrainType::Unknown {
                applied += 1;
            }
            reg.set_type(info.number, typ, now);
        }
    }
    Ok(applied)
}

/// Stream the journey response for `number`, scan for the first
/// `"categoryCode":"…"`, and apply the result to the registry. Reads at most
/// [`SERVICE_SCAN_LIMIT`] bytes before giving up so we never buffer the 30 KB
/// of stops we don't care about.
async fn fetch_service<'a, T, D>(
    http: &mut HttpClient<'a, T, D>,
    http_buf: &mut [u8],
    number: u32,
    registry: &SharedRegistry,
) -> Result<ServiceType, FetchError>
where
    T: embedded_nal_async::TcpConnect + 'a,
    D: embedded_nal_async::Dns + 'a,
{
    let mut url: String<160> = String::new();
    let _ = write!(url, "https://{HOST}/reisinformatie-api/api/v2/journey?train={number}");

    let headers = [("Ocp-Apim-Subscription-Key", API_KEY), ("Accept", "application/json")];
    let req = http.request(Method::GET, &url).await.map_err(FetchError::Http)?;
    let mut req = req.headers(&headers);
    let resp = req.send(http_buf).await.map_err(FetchError::Http)?;

    let status = resp.status;
    if !status.is_successful() {
        // Non-2xx (incl. 404 "Deze trein kan niet gevonden") - stamp the train as attempted so we honour the cooldown
        let now = Instant::now();
        let mut reg = registry.lock().await;
        reg.set_service(number, ServiceType::Unknown, now);
        return Err(FetchError::HttpStatus(status.0));
    }

    let mut reader = resp.body().reader();
    let service = scan_category(&mut reader).await?;

    let now = Instant::now();
    {
        let mut reg = registry.lock().await;
        reg.set_service(number, service, now);
    }
    Ok(service)
}

/// Scan the streamed body for the first `"categoryCode":"XYZ"`. Returns the
/// mapped [`ServiceType`] (Unknown if the field is absent or carries a code
/// we don't track).
async fn scan_category<R>(reader: &mut R) -> Result<ServiceType, FetchError>
where
    R: Read,
    R::Error: core::fmt::Debug,
{
    const NEEDLE: &[u8] = b"\"categoryCode\":\"";
    const OVERLAP: usize = NEEDLE.len() - 1;
    // Window must be large enough to read meaningful chunks while keeping a
    // small carry-over from the previous read so the needle (and its short
    // value) can span chunk boundaries.
    let mut buf = [0u8; 512];
    let mut head = 0usize;
    let mut total = 0usize;

    loop {
        if total >= SERVICE_SCAN_LIMIT {
            return Ok(ServiceType::Unknown);
        }
        let n = reader.read(&mut buf[head..]).await.map_err(|_| FetchError::Stream)?;
        if n == 0 {
            return Ok(ServiceType::Unknown);
        }
        total += n;
        let filled = head + n;

        if let Some(pos) = find_subslice(&buf[..filled], NEEDLE) {
            let value_start = pos + NEEDLE.len();
            if let Some(end) = buf[value_start..filled].iter().position(|&b| b == b'"') {
                return Ok(map_category_code(&buf[value_start..value_start + end]));
            }
            // Closing quote not in this chunk — shift the partial value to
            // the start and switch to "scan for `\"`" mode.
            let keep = filled - value_start;
            buf.copy_within(value_start..filled, 0);
            head = keep;
            loop {
                if total >= SERVICE_SCAN_LIMIT {
                    return Ok(ServiceType::Unknown);
                }
                let n = reader.read(&mut buf[head..]).await.map_err(|_| FetchError::Stream)?;
                if n == 0 {
                    return Ok(ServiceType::Unknown);
                }
                total += n;
                let filled = head + n;
                if let Some(end) = buf[..filled].iter().position(|&b| b == b'"') {
                    return Ok(map_category_code(&buf[..end]));
                }
                // Service codes are 2–5 chars; if we filled the whole
                // buffer without seeing a quote the response is malformed.
                if filled == buf.len() {
                    return Ok(ServiceType::Unknown);
                }
                head = filled;
            }
        }

        // Needle not found; preserve the last OVERLAP bytes so a match that
        // straddles the chunk boundary survives the next read.
        if filled >= OVERLAP {
            buf.copy_within(filled - OVERLAP..filled, 0);
            head = OVERLAP;
        } else {
            head = filled;
        }
    }
}

/// Tiny byte-substring search. The body is small enough that a naive scan
/// (~hay·needle worst case) is cheaper than dragging in a generic algorithm.
fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > hay.len() {
        return None;
    }
    (0..=hay.len() - needle.len()).find(|&i| &hay[i..i + needle.len()] == needle)
}

fn map_category_code(code: &[u8]) -> ServiceType {
    match code {
        b"SPR" => ServiceType::Sprinter,
        b"IC" => ServiceType::Intercity,
        b"ICD" => ServiceType::IntercityDirect,
        _ => {
            log::info!("ns_api: unknown train service string {:?}", code);
            ServiceType::Unknown
        }
    }
}

fn map_train_type(s: Option<&str>) -> TrainType {
    match s {
        None => TrainType::Unknown,
        Some(s) => {
            // NS returns mixed-case strings like "Flirt", "VIRM-VI", "ICM-III".
            // Strip subtype suffix and normalize to uppercase before matching.
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
    }
}
