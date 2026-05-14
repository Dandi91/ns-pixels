#![no_std]
#![feature(allocator_api)]

extern crate alloc;

pub mod decompress;
pub mod display;
pub mod feed;
pub mod input;
pub mod ns_api;
pub mod projection;
pub mod registry;
pub mod train;
pub mod xml_parser;
pub mod zmq;

/// Allocate `len` zeroed bytes in PSRAM and leak the box for a `&'static` borrow.
/// Used for one-shot setup buffers; the tasks run forever so we never reclaim them.
pub fn leak_psram_slice(len: usize) -> &'static mut [u8] {
    let b = alloc::boxed::Box::<[u8], _>::new_uninit_slice_in(len, esp_alloc::ExternalMemory);
    // SAFETY: u8 has no validity invariants; smoltcp/zmq write before reading.
    let b = unsafe { b.assume_init() };
    alloc::boxed::Box::leak(b)
}
