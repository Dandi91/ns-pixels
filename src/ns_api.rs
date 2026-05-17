//! NS API enrichment task.
//!
//! Drains the new-train channel and pulls additional retry candidates from
//! [`Registry::pending_enrichment`], merging both into a single buffer of
//! [`EnrichmentRequest`]s. Per iteration, one TLS connection is opened to
//! `gateway.apiportal.ns.nl` and reused across every request in the buffer,
//! amortizing the handshake cost over the whole batch.
//!
//! Each request is one of:
//!
//! - **Type** — `GET /virtual-train-api/v1/trein?ids=N`, ~150 B body, parsed
//!   as JSON.
//! - **Service** — `GET /reisinformatie-api/api/v2/journey?train=N`, ~30 KB
//!   body, streamed and scanned for the first `"categoryCode":"…"` before the
//!   tail is drained off the connection.
//!
//! Both kinds apply their result to the shared [`SharedRegistry`]; a cluster
//! snapshot is published once the buffer is fully processed.

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
use crate::registry::{EnrichmentRequest, SharedRegistry};
use crate::train::{ServiceType, TrainType};

pub const QUEUE_CAPACITY: usize = 64;
/// Total enrichment requests handled per wake-up. Each one is a single HTTPS
/// round-trip on the shared keep-alive connection (Type requests are further
/// batched up to [`BATCH_MAX`]).
const BUFFER_CAP: usize = 16;
/// Max train numbers per `/virtual-train-api/v1/trein?ids=…` call.
const BATCH_MAX: usize = 8;
/// Upper bound on bytes pulled from the journey response before giving up on
/// finding `categoryCode`. The field reliably appears within the first stop;
/// 8 KiB is plenty of headroom.
const SERVICE_SCAN_LIMIT: usize = 8 * 1024;

/// Brief window after a wake-up to let the registry settle before sweeping.
const COALESCE: Duration = Duration::from_millis(500);
/// Fallback wake even if no new-train notifications arrive — picks up trains
/// dropped from the queue under load and retries failed/missing entries.
const SWEEP_INTERVAL: Duration = Duration::from_secs(5);
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

    let mut buf: Vec<EnrichmentRequest, { BUFFER_CAP * 2 }> = Vec::new();
    let mut type_batch: Vec<u32, BUFFER_CAP> = Vec::new();
    let mut service_batch: Vec<u32, BUFFER_CAP> = Vec::new();

    // Initial delay to let the registry settle before starting enrichment.
    Timer::after(Duration::from_secs(2)).await;

    // One TLS session per non-empty wake-up: built right before use and
    // dropped right after. This keeps handshake amortization within a batch
    // (BATCH_MAX type-requests + up to BUFFER_CAP service requests share
    // one TLS connection) while avoiding idle sessions across batches.
    loop {
        // Wake on either a new-train notification (low-latency path) or the periodic sweep
        // (covers IDs dropped from a full channel and retries failed lookups).
        let _ = select(queue.receive(), Timer::after(SWEEP_INTERVAL)).await;

        // Brief settle window so a burst of notifications coalesces.
        Timer::after(COALESCE).await;

        // Build the request buffer. New trains land here first as a (Type, Service) pair each;
        // pending_enrichment then tops up the rest with retry candidates from the registry.
        buf.clear();
        while buf.capacity() - buf.len() >= 2 {
            match queue.try_receive() {
                Ok(n) => {
                    let _ = buf.push(EnrichmentRequest::Type(n));
                    let _ = buf.push(EnrichmentRequest::Service(n));
                }
                Err(_) => break,
            }
        }
        // Only check for pending enrichment if there were no new trains.
        // Otherwise, some trains may be requested twice
        if buf.is_empty() {
            let reg = registry.lock().await;
            reg.pending_enrichment(&mut buf, RETRY_COOLDOWN);
        }
        if buf.is_empty() {
            continue;
        }

        // Partition by axis so Type requests can be coalesced into batched URL calls;
        // Service requests stay one-per-call (the journey endpoint takes a single train).
        type_batch.clear();
        service_batch.clear();
        for &req in buf.iter() {
            match req {
                EnrichmentRequest::Type(n) => {
                    let _ = type_batch.push(n);
                }
                EnrichmentRequest::Service(n) => {
                    let _ = service_batch.push(n);
                }
            }
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
            match fetch_types_on(&mut resource, http_buf, chunk, registry).await {
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
                match fetch_service_on(&mut resource, http_buf, n, registry).await {
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

    let headers = [("Ocp-Apim-Subscription-Key", API_KEY), ("Accept", "application/json")];
    let resp = resource
        .get(&path)
        .headers(&headers)
        .send(http_buf)
        .await
        .map_err(FetchError::Http)?;

    let status = resp.status;
    if !status.is_successful() {
        let mut reader = resp.body().reader();
        let _ = drain(&mut reader).await;
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

/// Stream the journey response for `number` over an already-open keep-alive
/// connection, scan for the first `"categoryCode":"…"`, drain the rest of the
/// body so the connection is reusable, and apply the result to the registry.
/// At most [`SERVICE_SCAN_LIMIT`] bytes are scanned before bailing; the
/// remaining body bytes are still drained so the next request on this
/// connection sees clean framing.
async fn fetch_service_on<'res, C>(
    resource: &mut reqwless::client::HttpResource<'res, C>,
    http_buf: &mut [u8],
    number: u32,
    registry: &SharedRegistry,
) -> Result<ServiceType, FetchError>
where
    C: Read + Write,
{
    let mut path: String<128> = String::new();
    let _ = write!(path, "/reisinformatie-api/api/v2/journey?train={number}");

    let headers = [("Ocp-Apim-Subscription-Key", API_KEY), ("Accept", "application/json")];
    let resp = resource
        .get(&path)
        .headers(&headers)
        .send(http_buf)
        .await
        .map_err(FetchError::Http)?;

    let status = resp.status;
    if !status.is_successful() {
        // Non-2xx (incl. 404 "Deze trein kan niet gevonden") - stamp the train
        // as attempted so we honor the cooldown. The body is short for error responses,
        // so we still drain it to keep the connection alive.
        let mut reader = resp.body().reader();
        let _ = drain(&mut reader).await;
        let now = Instant::now();
        let mut reg = registry.lock().await;
        reg.set_service(number, ServiceType::Unknown, now);
        return Err(FetchError::HttpStatus(status.0));
    }

    let mut reader = resp.body().reader();
    let service = scan_category(&mut reader).await?;
    // Drain the remaining body so the next request on this keep-alive
    // connection doesn't read stale bytes as its response headers. If draining
    // errors, propagate so the caller drops the resource and reconnects.
    drain(&mut reader).await.map_err(|_| FetchError::Stream)?;

    let now = Instant::now();
    {
        let mut reg = registry.lock().await;
        reg.set_service(number, service, now);
    }
    Ok(service)
}

/// Read and discard everything remaining on `reader` until EOF. Required after
/// [`scan_category`] aborts early, so the keep-alive connection stays framed.
async fn drain<R>(reader: &mut R) -> Result<(), R::Error>
where
    R: Read,
{
    let mut sink = [0u8; 256];
    loop {
        let n = reader.read(&mut sink).await?;
        if n == 0 {
            return Ok(());
        }
    }
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
        b"ICD" | b"ECD" => ServiceType::IntercityDirect,
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
