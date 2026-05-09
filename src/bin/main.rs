#![no_std]
#![no_main]
#![feature(allocator_api)]

extern crate alloc;

use embassy_executor::Spawner;
use embassy_net::{
    Runner, StackResources,
    dns::DnsQueryType,
    dns::DnsSocket,
    tcp::TcpSocket,
};
use embassy_time::{Duration, Instant, Timer};
use esp_alloc as _;
use esp_backtrace as _;
use esp_hal::{
    clock::CpuClock, interrupt::software::SoftwareInterruptControl, ram, rng::Rng,
    timer::timg::TimerGroup,
};
use esp_hub75::Hub75Pins16;
use esp_println::println;
use esp_radio::wifi::{Config, ControllerConfig, Interface, WifiController, sta::StationConfig};
use ns_pixels::{
    decompress::Decompressor,
    ns_api::{self, NewTrainQueue},
    projection::wgs84_to_matrix,
    registry::{Registry, SharedRegistry},
    xml_parser::{self, Train},
    zmq,
};

esp_bootloader_esp_idf::esp_app_desc!();

// When you are okay with using a nightly compiler it's better to use https://docs.rs/static_cell/2.1.0/static_cell/macro.make_static.html
macro_rules! mk_static {
    ($t:ty,$val:expr) => {{
        static STATIC_CELL: static_cell::StaticCell<$t> = static_cell::StaticCell::new();
        #[deny(unused_attributes)]
        let x = STATIC_CELL.uninit().write(($val));
        x
    }};
}

const SSID: &str = env!("SSID");
const PASSWORD: &str = env!("PASSWORD");

#[esp_rtos::main]
async fn main(spawner: Spawner) -> ! {
    esp_println::logger::init_logger_from_env();
    let config = esp_hal::Config::default().with_cpu_clock(CpuClock::max());
    let peripherals = esp_hal::init(config);

    esp_alloc::heap_allocator!(#[ram(reclaimed)] size: 64 * 1024);
    esp_alloc::heap_allocator!(size: 36 * 1024);
    // ESP32-S3-WROOM-1 module: 2 MiB external PSRAM, registered as an
    // additional heap region. Slower than internal SRAM, so latency-sensitive
    // buffers should still live in .bss.
    esp_alloc::psram_allocator!(peripherals.PSRAM, esp_hal::psram);

    let timg0 = TimerGroup::new(peripherals.TIMG0);
    let sw_int = SoftwareInterruptControl::new(peripherals.SW_INTERRUPT);
    esp_rtos::start(timg0.timer0, sw_int.software_interrupt0);

    let station_config = Config::Station(
        StationConfig::default()
            .with_ssid(SSID)
            .with_password(PASSWORD.into()),
    );

    println!("Starting wifi");
    let (controller, interfaces) = esp_radio::wifi::new(
        peripherals.WIFI,
        ControllerConfig::default().with_initial_config(station_config),
    )
    .unwrap();
    println!("Wifi configured and started!");

    let wifi_interface = interfaces.station;

    let config = embassy_net::Config::dhcpv4(Default::default());

    let rng = Rng::new();
    let seed = (rng.random() as u64) << 32 | rng.random() as u64;

    // Init network stack
    let (stack, runner) = embassy_net::new(
        wifi_interface,
        config,
        mk_static!(StackResources<4>, StackResources::<4>::new()),
        seed,
    );

    spawner.spawn(connection(controller).unwrap());
    spawner.spawn(net_task(runner).unwrap());

    stack.wait_config_up().await;

    if let Some(config) = stack.config_v4() {
        println!("Got IP: {}", config.address);
    }

    let peer_ip = {
        let dns_client = DnsSocket::new(stack);
        let result = dns_client
            .query("pubsub.besteffort.ndovloket.nl", DnsQueryType::A)
            .await;
        result.unwrap()[0]
    };
    println!("Got peer IP: {}", peer_ip);

    let rx_buf = mk_static!([u8; 4096], [0u8; 4096]);
    let tx_buf = mk_static!([u8; 4096], [0u8; 4096]);
    let mut socket = TcpSocket::new(stack, rx_buf, tx_buf);
    socket.connect((peer_ip, 7664)).await.unwrap();
    let mut sub = zmq::Subscriber::new(socket, 64 * 1024).await.unwrap();
    sub.subscribe(b"/RIG/NStreinpositiesInterface5")
        .await
        .unwrap();

    // Decompressor owns the InflateState (~43 KiB) and a 400 KiB output buffer, both in PSRAM.
    // Typical decompressed XML is ~300 KiB; 400 KiB gives headroom for busy moments.
    let mut decompressor = Decompressor::new(400 * 1024);
    println!(
        "decompressor allocated in PSRAM ({} KiB buffer)",
        decompressor.capacity() / 1024
    );

    // Per-message scratch space, reused across iterations. PSRAM-backed
    // because train counts can spike into the thousands.
    let mut trains: alloc::vec::Vec<Train, _> = alloc::vec::Vec::new_in(esp_alloc::ExternalMemory);

    // Live registry + new-train channel, both static so the API task can
    // borrow them. The registry itself lives on the default (internal SRAM)
    // heap for cache-friendly iteration during rendering.
    let registry: &'static SharedRegistry =
        mk_static!(SharedRegistry, SharedRegistry::new(Registry::new()));
    let new_train_q: &'static NewTrainQueue = mk_static!(NewTrainQueue, NewTrainQueue::new());
    spawner.spawn(ns_api::run(stack, registry, new_train_q, seed).unwrap());

    // Trains not seen in this many seconds are dropped from the registry.
    const STALE_AFTER: Duration = Duration::from_secs(60);

    loop {
        let frames = sub.recv().await.unwrap();
        if frames.len() < 2 {
            println!("unexpected frame count: {}", frames.len());
            continue;
        }
        let payload = &frames[1];

        let start = Instant::now();
        let xml = match decompressor.inflate_gzip(payload) {
            Ok(s) => s,
            Err(e) => {
                println!("decompress error: {:?}", e);
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
        {
            let mut reg = registry.lock().await;
            if cutoff != now {
                evicted = reg.evict_older_than(cutoff);
            }
            for t in &trains {
                let pixel = wgs84_to_matrix(t.lat, t.lon);
                if reg.upsert(t.number, pixel, now) {
                    new_count += 1;
                    if new_train_q.try_send(t.number).is_err() {
                        dropped += 1;
                    }
                }
            }
            registry_len = reg.len();
            unknowns = reg.unknown_count();
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

#[embassy_executor::task]
async fn connection(mut controller: WifiController<'static>) {
    println!("start connection task");

    loop {
        println!("About to connect...");

        match controller.connect_async().await {
            Ok(info) => {
                println!("Wifi connected to {:?}", info);

                // wait until we're no longer connected
                let info = controller.wait_for_disconnect_async().await.ok();
                println!("Disconnected: {:?}", info);
            }
            Err(e) => {
                println!("Failed to connect to wifi: {e:?}");
            }
        }

        Timer::after(Duration::from_millis(5000)).await
    }
}

#[embassy_executor::task]
async fn net_task(mut runner: Runner<'static, Interface<'static>>) {
    runner.run().await
}
