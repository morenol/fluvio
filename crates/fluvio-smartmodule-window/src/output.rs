use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::link::smartmodule::SmartModuleWindowRuntimeError;
use fluvio_protocol::record::Record;

/// A type used to return processed records and/or an error from a SmartModule
#[derive(Debug, Default, Encoder, Decoder)]
pub struct SmartModuleWindowOutput {
    /// The successfully processed output Records
    pub successes: Vec<Record>,
    /// Any runtime error if one was encountered
    pub error: Option<SmartModuleWindowRuntimeError>,
}
