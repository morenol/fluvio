mod vehicle;
mod host;

use std::sync::{OnceLock, Mutex};

use fluvio_smartmodule::{
    smartmodule, Record, Result, eyre,
    dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleWindowInput},
    RecordData,
};

use fluvio_smartmodule_window::window::TumblingWindow;
use vehicle::{MQTTEvent, DefaultWindowState, VehicleStatistics};


#[smartmodule(init)]
fn init(_params: SmartModuleExtraParams) -> Result<()> {
    STATE
        .set(Mutex::new(TumblingWindow::new()))
        .map_err(|err| eyre!("state init: {:#?}", err))
}

static STATE: OnceLock<Mutex<DefaultWindowState>> = OnceLock::new();

#[smartmodule(filter_map)]
pub fn filter_map(record: &Record) -> Result<Option<(Option<RecordData>, RecordData)>> {
    let mqtt: MQTTEvent = serde_json::from_slice(record.value.as_ref())?;
    let event = mqtt.payload.VP;

    // for now emit same event

    let key = event.veh.to_string();

    // add to state
    let mut stats = STATE.get().unwrap().lock().unwrap();
    stats.add(event.veh, &event);

    let summary: Vec<&VehicleStatistics> = stats.summary();

    Ok(Some((
        None,
        RecordData::from(serde_json::to_string(&summary)?),
    )))
}

#[smartmodule(window)]
fn window(input: SmartModuleWindowInput) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    todo!()
}

// TODO: window API. this need to be called by
/*
Acmmulated State (Table ), Result
{
    [
        "veh": 116,
        "avg_speed": 3.2
    ],
    [
        "veh": 117,
        "avg_speed": 3.2
    ],
    [
        "veh": 118,
        "avg_speed": 3.2
    ]
}
#[smartmodule(window(fetch))]
// FetchFlag = { All, Range }
pub fn window_fetch(time: Time, flag: FetchFlag) -> Resul<WindowStateSummary>> {
    //
}

#[smartmodule(window(slide))]
pub fn window_move(time: Time) -> Result<Option<RecordData>> {
    //
}
*/
