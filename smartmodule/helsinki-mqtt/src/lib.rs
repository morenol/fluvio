mod vehicle;
//mod host;

use std::sync::{OnceLock, Mutex};

use fluvio_smartmodule::{
    smartmodule, Record, Result, eyre,
    dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleWindowInput},
    RecordData,
};

use fluvio_smartmodule_window::window::NoKeySelector;
use vehicle::{MQTTEvent, DefaultWindowState, VehicleStatistics};

#[smartmodule(init)]
fn init(_params: SmartModuleExtraParams) -> Result<()> {
    let window_state = DefaultWindowState::builder()
        .window_size_sec(60 as u16)
        .key_selector(NoKeySelector::default())
        .value_selector(NoKeySelector::default())
        .build()
        .map_err(|err| eyre!("tumbling window init: {:#?}", err))?;

    STATE
        .set(Mutex::new(window_state))
        .map_err(|err| eyre!("state init: {:#?}", err))
}

static STATE: OnceLock<Mutex<DefaultWindowState>> = OnceLock::new();

#[smartmodule(filter_map)]
pub fn filter_map(record: &Record) -> Result<Option<(Option<RecordData>, RecordData)>> {
    let mqtt: MQTTEvent = serde_json::from_slice(record.value.as_ref())?;
    if let Some(vp) = mqtt.payload.VP {
        if vp.spd.is_some() {
            let mut stats = STATE.get().unwrap().lock().unwrap();
            if let Some(window_completed) = stats
                .add(vp.clone())
                .map_err(|err| eyre!("add: {:#?}", err))?
            {
                let summary = window_completed.summary();

                Ok(Some((
                    None,
                    RecordData::from(serde_json::to_string(&summary)?),
                )))
            } else {
                /*
                Ok(Some((
                    None,
                    RecordData::from(serde_json::to_string(&vp)?),
                )))
                */
                Ok(None)
            }

            /*
            Ok(Some((
                None,
                RecordData::from(serde_json::to_string(&vp)?),
            )))
            */
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/*
#[smartmodule(window)]
fn window(input: SmartModuleWindowInput) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    todo!()
}
*/

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
