mod event;

use std::sync::{OnceLock, Mutex};

use cloudevents::{Event, AttributesReader, Data};

use fluvio_smartmodule::{
    smartmodule, Record, Result, eyre,
    dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleWindowInput},
    RecordData,
};

use event::{OpenMeterEvent, DefaultWindowState};

#[smartmodule(init)]
fn init(_params: SmartModuleExtraParams) -> Result<()> {
    STATE
        .set(Mutex::new(DefaultWindowState::new(10, "path".to_owned())))
        .map_err(|err| eyre!("state init: {:#?}", err))
}

static STATE: OnceLock<Mutex<DefaultWindowState>> = OnceLock::new();

/*
#[smartmodule(filter_map)]
pub fn filter_map(record: &Record) -> Result<Option<(Option<RecordData>, RecordData)>> {
    let cloud_event: Event = serde_json::from_slice(record.value.as_ref())?;

    let mut stats = STATE.get().unwrap().lock().unwrap();
    if let Some(window_completed) = stats.add(cloud_event) {
        let summary = window_completed.summary();

        Ok(Some((
            None,
            RecordData::from(serde_json::to_string(&summary)?),
        )))
    } else {

        Ok(None)
    }
}
*/
