//! NS API enrichment task.
//!
//! Each wake-up drains the new-train channel into a pair of pending-work
//! buffers — one per request kind — and tops them up with retry candidates
//! pulled from [`crate::registry::Registry::pending_enrichment`]. The two
//! axes (train type and service category) are tracked independently so a
//! resolved type doesn't gate a still-missing service, and vice versa; this
//! also lets the buffers be sized to each endpoint's actual cost.
//!
//! Per iteration, one TLS connection is opened to `gateway.apiportal.ns.nl`
//! and reused across every request in the batch, amortizing the handshake
//! cost. After the type batch (chunked to [`BATCH_MAX`] ids per call) comes
//! the service batch (one request per train, since the journey endpoint
//! takes a single train number).
//!
//! Two kinds of request:
//!
//! - **Type** — `GET /virtual-train-api/v1/trein?ids=N1,N2,…`, ~150 B/train,
//!   parsed as JSON.
//! - **Service** — `GET /reisinformatie-api/api/v2/journey?train=N`, ~30 KB
//!   body, read fully into the HTTP buffer and deserialized with serde to
//!   pull out the first `categoryCode`.
//!
//! Both kinds apply their result to the shared [`SharedRegistry`]; a cluster
//! snapshot is published once the whole batch is processed.

use core::fmt::Write as _;

use embassy_futures::select::select;
use embassy_net::Stack;
use embassy_net::dns::DnsSocket;
use embassy_net::tcp::client::{TcpClient, TcpClientState};
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::Channel;
use embassy_time::{Duration, Instant, Timer};
use embedded_io_async::{Read, Write};
use heapless::{String, Vec};
use reqwless::{
    client::{HttpClient, TlsConfig, TlsVerify},
    request::RequestBuilder,
};
use serde::Deserialize;

use crate::display;
use crate::leak_psram_slice;
use crate::map_mode;
use crate::registry::SharedRegistry;
use crate::train::{ServiceType, TrainType};

pub const QUEUE_CAPACITY: usize = 64;
/// Pending-work buffer sizes — picked per axis based on per-request cost.
/// Type requests coalesce up to [`BATCH_MAX`] ids per HTTPS call, so a larger
/// buffer is cheap. Service requests are one-per-call against the ~30 KB
/// journey endpoint, so the buffer is kept small to bound per-wake latency.
const TYPE_BUFFER_CAP: usize = 32;
const SERVICE_BUFFER_CAP: usize = 4;
/// Max train numbers per `/virtual-train-api/v1/trein?ids=…` call.
const BATCH_MAX: usize = 8;
/// Brief window after a wake-up to let the registry settle before sweeping.
const COALESCE: Duration = Duration::from_millis(100);
/// Fallback wake even if no new-train notifications arrive — picks up trains
/// dropped from the queue under load and retries failed/missing entries.
const SWEEP_INTERVAL: Duration = Duration::from_secs(2);
/// Pause between API calls if a previous call failed; avoids hammering the
/// gateway when something is broken.
const FAILURE_BACKOFF: Duration = Duration::from_secs(10);
/// Minimum gap between enrichment attempts for the same train number. Trains
/// whose info the API never returns (or returns in an unparseable form) sit on
/// this cooldown instead of being re-requested every sweep.
const RETRY_COOLDOWN: Duration = Duration::from_secs(120);

/// Base URL used for [`HttpClient::resource`] — keeping the connection open
/// across every enrichment request in the buffer avoids redoing the TLS
/// handshake for each one.
const BASE_URL: &str = "https://gateway.apiportal.ns.nl";

// TLS record buffers (~17 KiB each; required for max TLS fragment).
const TLS_BUF_LEN: usize = 17 * 1024;
// HTTP response buffer. Sized for the journey endpoint (~30 KB per response)
// with comfortable headroom so the full body can be read into memory before
// serde deserialization.
const HTTP_BUF_LEN: usize = 64 * 1024;

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

// Bounds for the journey response payload. NS journeys typically have ~10–20
// stops with one departure and arrival each; the caps below leave headroom
// without inflating the parsed structure beyond a few KB.
const MAX_STOPS: usize = 64;
const MAX_EVENTS: usize = 4;

#[derive(Deserialize)]
struct JourneyResp<'a> {
    #[serde(borrow)]
    payload: JourneyPayload<'a>,
}

#[derive(Deserialize)]
struct JourneyPayload<'a> {
    #[serde(borrow)]
    stops: Vec<JourneyStop<'a>, MAX_STOPS>,
}

#[derive(Deserialize)]
struct JourneyStop<'a> {
    #[serde(borrow, default)]
    departures: Vec<JourneyEvent<'a>, MAX_EVENTS>,
    #[serde(borrow, default)]
    arrivals: Vec<JourneyEvent<'a>, MAX_EVENTS>,
}

#[derive(Deserialize, Default)]
struct JourneyEvent<'a> {
    #[serde(borrow, default)]
    product: Option<JourneyProduct<'a>>,
}

#[derive(Deserialize)]
struct JourneyProduct<'a> {
    #[serde(rename = "categoryCode", borrow, default)]
    category_code: Option<&'a str>,
}

#[embassy_executor::task]
pub async fn run(
    stack: Stack<'static>,
    registry: &'static SharedRegistry,
    queue: &'static NewTrainQueue,
    tls_seed: u64,
    api_key: &'static str,
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

    let headers = [
        ("Ocp-Apim-Subscription-Key", api_key),
        ("Accept", "application/json"),
    ];

    let mut type_batch: Vec<u32, TYPE_BUFFER_CAP> = Vec::new();
    let mut service_batch: Vec<u32, SERVICE_BUFFER_CAP> = Vec::new();

    // One TLS session per non-empty wake-up: built right before use and
    // dropped right after. This keeps handshake amortization within a batch
    // (the whole type batch plus the full service batch share one TLS
    // connection) while avoiding idle sessions across batches.
    loop {
        // Wake on either a new-train notification (low-latency path) or the periodic sweep
        // (covers IDs dropped from a full channel and retries failed lookups).
        select(queue.ready_to_receive(), Timer::after(SWEEP_INTERVAL)).await;

        // Brief settle window so a burst of notifications coalesces.
        Timer::after(COALESCE).await;

        // Build the request buffers. New trains land here first;
        // pending_enrichment then tops up the rest with retry candidates from the registry.
        type_batch.clear();
        service_batch.clear();
        while type_batch.len() < type_batch.capacity() {
            match queue.try_receive() {
                Ok(n) => {
                    let _ = type_batch.push(n);
                    let _ = service_batch.push(n);
                }
                Err(_) => break,
            }
        }
        // Only check for pending enrichment if there were no new trains.
        // Otherwise, some trains may be requested twice
        if type_batch.is_empty() {
            let reg = registry.lock().await;
            reg.pending_enrichment(&mut type_batch, &mut service_batch, RETRY_COOLDOWN);
        }
        if type_batch.is_empty() && service_batch.is_empty() {
            continue;
        }

        // Now that we have work, open a fresh TLS session and use it for
        // every request in this batch. Dropped at the end of the iteration.
        rng_seed = rng_seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let tls = TlsConfig::new(rng_seed, tls_read, tls_write, TlsVerify::None);
        let mut http = HttpClient::new_with_tls(&tcp_client, &dns, tls);
        let mut resource = match http.resource(BASE_URL).await {
            Ok(r) => r,
            Err(e) => {
                log::warn!("ns_api: resource open failed: {:?}", e);
                Timer::after(FAILURE_BACKOFF).await;
                continue;
            }
        };

        let mut failed = false;
        for chunk in type_batch.chunks(BATCH_MAX) {
            match fetch_types_on(&mut resource, http_buf, chunk, registry, &headers).await {
                Ok(applied) => log::info!("ns_api: type batch {}/{} resolved", applied, chunk.len()),
                Err(e) => {
                    log::warn!("ns_api: type batch {:?} failed: {:?}", chunk, e);
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            for &n in service_batch.iter() {
                match fetch_service_on(&mut resource, http_buf, n, registry, &headers).await {
                    Ok(s) => log::info!("ns_api: service {} -> {:?}", n, s),
                    Err(e) => {
                        log::warn!("ns_api: service {} failed: {:?}", n, e);
                        failed = true;
                        break;
                    }
                }
            }
        }
        publish_snapshot(registry).await;
        if failed {
            Timer::after(FAILURE_BACKOFF).await;
        }
    }
}

/// Rebuild the cluster snapshot and hand it to the display, if a free buffer
/// is available. Skipping when none is free is fine — the next refresh catches up.
async fn publish_snapshot(registry: &SharedRegistry) {
    if let Some(buf) = display::try_take_free_clusters() {
        {
            let reg = registry.lock().await;
            reg.rebuild_clusters_into(buf, map_mode::current());
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
}

/// Fetch train-type records for up to [`BATCH_MAX`] ritnummers in a single
/// `?ids=N1,N2,…` call over the shared keep-alive connection. Applies each
/// resolved [`TrainType`] (or [`TrainType::Unknown`] if NS has no
/// well-formed record) to the registry, stamping the cooldown for every
/// requested train. Returns the count of trains that resolved to a concrete
/// type.
async fn fetch_types_on<'res, C>(
    resource: &mut reqwless::client::HttpResource<'res, C>,
    http_buf: &mut [u8],
    batch: &[u32],
    registry: &SharedRegistry,
    headers: &[(&str, &str)],
) -> Result<usize, FetchError>
where
    C: Read + Write,
{
    debug_assert!(!batch.is_empty() && batch.len() <= BATCH_MAX);

    // Max length: base (~32) + BATCH_MAX*7 digits + 9 commas ≈ 110.
    let mut path: String<160> = String::new();
    let _ = write!(path, "/virtual-train-api/v1/trein?ids=");
    for (i, id) in batch.iter().enumerate() {
        if i > 0 {
            let _ = path.push(',');
        }
        let _ = write!(path, "{}", id);
    }

    let resp = resource
        .get(&path)
        .headers(headers)
        .send(http_buf)
        .await
        .map_err(FetchError::Http)?;

    let status = resp.status;
    if !status.is_successful() {
        let _ = resp.body().read_to_end().await;
        // Still stamp every batch member so the cooldown is honored,
        // and we don't immediately retry on the next sweep.
        let now = Instant::now();
        let mut reg = registry.lock().await;
        for &n in batch {
            reg.set_type(n, TrainType::Unknown, now);
        }
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

/// Fetch the journey response for `number` over an already-open keep-alive
/// connection, deserialize it with serde, and apply the resolved
/// [`ServiceType`] to the registry. The first `categoryCode` found while
/// walking the parsed stops (departures first, then arrivals) wins.
async fn fetch_service_on<'res, C>(
    resource: &mut reqwless::client::HttpResource<'res, C>,
    http_buf: &mut [u8],
    number: u32,
    registry: &SharedRegistry,
    headers: &[(&str, &str)],
) -> Result<ServiceType, FetchError>
where
    C: Read + Write,
{
    let mut path: String<128> = String::new();
    let _ = write!(path, "/reisinformatie-api/api/v2/journey?train={number}");

    let resp = resource
        .get(&path)
        .headers(headers)
        .send(http_buf)
        .await
        .map_err(FetchError::Http)?;

    let status = resp.status;
    if !status.is_successful() {
        // Non-2xx (incl. 404 "Deze trein kan niet gevonden") - stamp the train
        // as attempted so we honor the cooldown. read_to_end consumes the body
        // so the keep-alive connection stays framed.
        let _ = resp.body().read_to_end().await;
        let now = Instant::now();
        let mut reg = registry.lock().await;
        reg.set_service(number, ServiceType::Unknown, now);
        return Err(FetchError::HttpStatus(status.0));
    }

    let body = resp.body().read_to_end().await.map_err(FetchError::Http)?;
    let body_str = core::str::from_utf8(body).map_err(|_| FetchError::InvalidUtf8)?;

    let (parsed, _): (JourneyResp, usize) = serde_json_core::from_str(body_str).map_err(|_| FetchError::Json)?;

    let service = parsed
        .payload
        .stops
        .iter()
        .flat_map(|s| s.departures.iter().chain(s.arrivals.iter()))
        .filter_map(|e| e.product.as_ref().and_then(|p| p.category_code))
        .next()
        .map(map_category_code)
        .unwrap_or(ServiceType::Unknown);

    let now = Instant::now();
    {
        let mut reg = registry.lock().await;
        reg.set_service(number, service, now);
    }
    Ok(service)
}

fn map_category_code(code: &str) -> ServiceType {
    match code {
        "SPR" => ServiceType::Sprinter,
        "IC" => ServiceType::Intercity,
        "ICD" | "ECD" => ServiceType::IntercityDirect,
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
