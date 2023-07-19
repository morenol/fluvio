use fluvio_smartmodule::{smartmodule, Record, RecordData, state::SmartModuleState};
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;

#[derive(Clone)]
pub struct CounterState {
    count: u32,
}

impl SmartModuleState for CounterState {
    fn init(_params: SmartModuleExtraParams) -> anyhow::Result<Self> {
        Self { count: 0 }
    }
}

#[smartmodule(materialize)]
pub fn materialize(
    _current: &Record,
    state: &mut CounterState,
) -> fluvio_smartmodule::Result<Option<(Option<RecordData>, RecordData)>> {
    state.count += 1;
    Ok(Some((None, format!("count: {}", state.count).into())))
}
