//! NDOV `NStreinpositiesInterface5` consumer task.
//!
//! Owns the ZMQ TCP socket, the gzip decompressor, and the XML parser. Each
//! incoming message is decompressed, parsed, and applied to the shared
//! [`Registry`] under a brief lock. Newly-seen train numbers are signalled
//! to the enrichment task via the new-train channel.

use embassy_net::{
    Stack,
    dns::{DnsQueryType, DnsSocket},
    tcp::TcpSocket,
};
use embassy_time::{Duration, Instant};
use esp_println::println;

use crate::decompress::Decompressor;
use crate::display;
use crate::ns_api::NewTrainQueue;
use crate::projection::wgs84_to_matrix;
use crate::registry::SharedRegistry;
use crate::xml_parser::{self, Train};
use crate::{leak_psram_slice, zmq};

const HOST: &str = "pubsub.besteffort.ndovloket.nl";
const PORT: u16 = 7664;
const TOPIC: &[u8] = b"/RIG/NStreinpositiesInterface5";

/// Trains not seen in this many seconds are dropped from the registry.
const STALE_AFTER: Duration = Duration::from_secs(60 * 5);

/// PSRAM scratch sizes.
const TCP_RX_LEN: usize = 4096;
const TCP_TX_LEN: usize = 4096;
const ZMQ_FRAME_CAP: usize = 64 * 1024;
const XML_BUF_LEN: usize = 400 * 1024;

#[embassy_executor::task]
pub async fn run(
    stack: Stack<'static>,
    registry: &'static SharedRegistry,
    queue: &'static NewTrainQueue,
) {
    // Resolve the publisher in a scoped DnsSocket so its smoltcp slot is
    // released before we open the long-lived TCP connection.
    let peer_ip = {
        let dns = DnsSocket::new(stack);
        dns.query(HOST, DnsQueryType::A).await.unwrap()[0]
    };
    println!("feed: resolved {} -> {}", HOST, peer_ip);

    // TCP socket buffers — leaked into PSRAM via Box::leak so the resulting
    // &'static mut [u8] satisfies TcpSocket's borrow without a static_cell
    // dance inside the task.
    let rx_buf = leak_psram_slice(TCP_RX_LEN);
    let tx_buf = leak_psram_slice(TCP_TX_LEN);

    let mut socket = TcpSocket::new(stack, rx_buf, tx_buf);
    socket.connect((peer_ip, PORT)).await.unwrap();
    let mut sub = zmq::Subscriber::new(socket, ZMQ_FRAME_CAP).await.unwrap();
    sub.subscribe(TOPIC).await.unwrap();

    // Decompressor owns a ~43 KiB InflateState plus the XML output buffer
    // (default 400 KiB). Both live in PSRAM.
    let mut decompressor = Decompressor::new(XML_BUF_LEN);

    // Per-message scratch space, reused across iterations. PSRAM-backed
    // because train counts can spike into the thousands.
    let mut trains: alloc::vec::Vec<Train, _> = alloc::vec::Vec::new_in(esp_alloc::ExternalMemory);

    loop {
        let frames = sub.recv().await.unwrap();
        if frames.len() < 2 {
            println!("feed: unexpected frame count: {}", frames.len());
            continue;
        }
        let payload = &frames[1];

        let start = Instant::now();
        let xml = match decompressor.inflate_gzip(payload) {
            Ok(s) => s,
            Err(e) => {
                println!("feed: decompress error: {:?}", e);
                continue;
            }
        };
        let inflate_ms = start.elapsed().as_millis();
        let xml_kib = xml.len() / 1024;

        trains.clear();
        xml_parser::parse(xml, |t| trains.push(t));
        let parse_ms = start.elapsed().as_millis() - inflate_ms;

        let now = Instant::now();
        let cutoff = now.checked_sub(STALE_AFTER).unwrap_or(now);

        let mut new_count = 0u32;
        let mut dropped = 0u32;
        let mut evicted = 0;
        let registry_len;
        let unknowns;
        // Grab a free snapshot buffer before locking; if the display hasn't
        // recycled the last one yet, we just skip publishing this round.
        let mut snapshot = display::try_take_free_clusters();
        {
            let mut reg = registry.lock().await;
            if cutoff != now {
                evicted = reg.evict_older_than(cutoff);
            }
            for t in &trains {
                let pixel = wgs84_to_matrix(t.lat, t.lon);
                if reg.upsert(t.number, pixel, now) {
                    new_count += 1;
                    if queue.try_send(t.number).is_err() {
                        dropped += 1;
                    }
                }
            }
            if let Some(buf) = snapshot.as_deref_mut() {
                reg.rebuild_clusters_into(buf);
            }
            registry_len = reg.len();
            unknowns = reg.unknown_count();
        }
        if let Some(buf) = snapshot {
            display::publish_clusters(buf);
        }
        let total_ms = start.elapsed().as_millis();

        println!(
            "{} KiB XML, {} trains: inflate {} ms, parse {} ms, total {} ms; \
             registry {} ({} unknown, +{} new, -{} stale, {} wake-skip)",
            xml_kib,
            trains.len(),
            inflate_ms,
            parse_ms,
            total_ms,
            registry_len,
            unknowns,
            new_count,
            evicted,
            dropped,
        );
    }
}
