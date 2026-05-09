#![no_std]
#![no_main]
#![feature(allocator_api)]

extern crate alloc;

use embassy_executor::Spawner;
use embassy_net::{
    Runner,
    StackResources,
    dns::DnsSocket,
    dns::DnsQueryType,
    tcp::client::{TcpClient, TcpClientState},
    tcp::TcpSocket,
};
use embassy_time::{Duration, Instant, Timer};
use esp_alloc as _;
use esp_backtrace as _;
use esp_hal::{
    clock::CpuClock,
    interrupt::software::SoftwareInterruptControl,
    ram,
    rng::Rng,
    timer::timg::TimerGroup,
};
use esp_println::println;
use esp_radio::wifi::{
    Config,
    ControllerConfig,
    Interface,
    WifiController,
    sta::StationConfig,
};
use reqwless::{
    client::HttpClient,
    request::{Method, RequestBuilder},
};
use miniz_oxide::{
    DataFormat,
    MZFlush,
    MZStatus,
    inflate::stream::{InflateState, inflate},
};
use esp_hub75::Hub75Pins16;
use ns_pixels::{gzip, train_xml, zmq};

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
        mk_static!(StackResources<3>, StackResources::<3>::new()),
        seed,
    );

    spawner.spawn(connection(controller).unwrap());
    spawner.spawn(net_task(runner).unwrap());

    stack.wait_config_up().await;

    if let Some(config) = stack.config_v4() {
        println!("Got IP: {}", config.address);
    }

    let dns_client = DnsSocket::new(stack);
    let result = dns_client.query("pubsub.besteffort.ndovloket.nl", DnsQueryType::A).await;
    let peer_ip = result.unwrap()[0];
    println!("Got peer IP: {}", peer_ip);

    let rx_buf = mk_static!([u8; 4096], [0u8; 4096]);
    let tx_buf = mk_static!([u8; 4096], [0u8; 4096]);
    let mut socket = TcpSocket::new(stack, rx_buf, tx_buf);
    socket.connect((peer_ip, 7664)).await.unwrap();
    let mut sub = zmq::Subscriber::new(socket, 64 * 1024).await.unwrap();
    sub.subscribe(b"/RIG/NStreinpositiesInterface5").await.unwrap();

    // ~43 KiB. Allocate in PSRAM (slower, but messages arrive every few
    // seconds so we don't care). Using the ExternalMemory allocator forces
    // the box to land in PSRAM rather than internal SRAM.
    let mut inflate_state = alloc::boxed::Box::<InflateState, _>::new_in(
        InflateState::new(DataFormat::Raw),
        esp_alloc::ExternalMemory,
    );
    println!("inflate state allocated in PSRAM");
    let mut inflate_out = [0u8; 2048];

    loop {
        let frames = sub.recv().await.unwrap();
        if frames.len() < 2 {
            println!("unexpected frame count: {}", frames.len());
            continue;
        }
        let payload = &frames[1];

        let start = Instant::now();
        let header_len = match gzip::skip_header(payload) {
            Some(n) => n,
            None => {
                println!("bad gzip header");
                continue;
            }
        };

        inflate_state.reset(DataFormat::Raw);
        let mut parser = train_xml::Parser::new();
        let mut count = 0u32;
        let mut input = &payload[header_len..];

        loop {
            let last = input.is_empty();
            let flush = if last { MZFlush::Finish } else { MZFlush::None };
            let res = inflate(&mut *inflate_state, input, &mut inflate_out, flush);
            input = &input[res.bytes_consumed..];
            if res.bytes_written > 0 {
                parser.feed(&inflate_out[..res.bytes_written], |t| {
                    count += 1;
                    // log::info!("train {} @ ({:.6}, {:.6})", t.nummer, t.lat, t.lon);
                });
            }
            match res.status {
                Ok(MZStatus::StreamEnd) => break,
                Ok(_) => {
                    if res.bytes_written == 0 && res.bytes_consumed == 0 {
                        break;
                    }
                }
                Err(e) => {
                    println!("inflate error: {:?}", e);
                    break;
                }
            }
        }
        println!("decoded {} trains in {} ms", count, start.elapsed().as_millis());
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
