#[cfg(all(feature = "k8", feature = "fs"))]
compile_error!("features `fluvio-sc/k8` and `fluvio-sc/fs` are mutually exclusive");

#[cfg(not(any(feature = "k8", feature = "fs")))]
compile_error!("one of `fluvio-sc/k8` or `fluvio-sc/fs` features is required");

#[macro_use]
pub mod config;
#[cfg(feature = "k8")]
pub mod k8;
pub mod cli;
pub mod core;
pub mod start;

pub mod stores;
mod init;
mod error;
mod services;
mod controllers;

#[cfg(feature = "k8")]
pub use init::start_main_loop;

const VERSION: &str = include_str!("../../../VERSION");

pub mod dispatcher {
    pub use fluvio_stream_dispatcher::*;
}
