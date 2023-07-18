mod event;

use std::sync::{OnceLock, Mutex};

use cloudevents::{Event, AttributesReader, Data};

use fluvio_smartmodule::{
    smartmodule, Record, Result, eyre,
    dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleWindowInput},
    RecordData,
};

use event::{OpenMeterEvent, DefaultTumblingWindow};

#[smartmodule(init)]
fn init(_params: SmartModuleExtraParams) -> Result<()> {
    let window_state = DefaultTumblingWindow::builder()
        .window_size_sec(10 as u16)
        //     .key_selector(NoKeySelector::default())
        .build()
        .map_err(|err| eyre!("tumbling window init: {:#?}", err))?;

    STATE
        .set(Mutex::new(window_state))
        .map_err(|err| eyre!("state init: {:#?}", err))
}

static STATE: OnceLock<Mutex<DefaultTumblingWindow>> = OnceLock::new();

#[smartmodule(filter_map)]
pub fn filter_map(record: &Record) -> Result<Option<(Option<RecordData>, RecordData)>> {
    let cloud_event: Event = serde_json::from_slice(record.value.as_ref())?;

    let mut stats = STATE.get().unwrap().lock().unwrap();
    if let Some(window_completed) = stats
        .add(cloud_event.into())
        .map_err(|err| eyre!("add: {:#?}", err))?
    {
        let summary = window_completed.summary();

        Ok(Some((
            None,
            RecordData::from(serde_json::to_string(&summary)?),
        )))
    } else {
        Ok(None)
    }
}
