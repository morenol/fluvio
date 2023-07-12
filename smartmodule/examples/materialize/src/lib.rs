use fluvio_smartmodule::{smartmodule, Record, RecordData, state::SmartModuleState};
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;
use fluvio_smartmodule::dataplane::core::{Encoder, Decoder};

#[derive(Default, Encoder, Decoder)]
pub struct CounterState {
    count: u32,
}

impl SmartModuleState for CounterState {
    fn init(_params: SmartModuleExtraParams) -> Self {
        Self::default()
    }
}

#[smartmodule(materialize)]
pub fn materialize(
    current: &Record,
    state: &mut CounterState,
) -> fluvio_smartmodule::Result<Option<(Option<RecordData>, RecordData)>> {
    state.count += 1;
    Ok(Some((None, format!("count: {}", state.count).into())))
}
