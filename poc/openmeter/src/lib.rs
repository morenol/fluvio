mod event;

use std::sync::{OnceLock, Mutex};

use fluvio_smartmodule::{
    smartmodule, Record, Result, eyre,
    dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleWindowInput},
    RecordData,
};

/*
#[smartmodule(init)]
fn init(_params: SmartModuleExtraParams) -> Result<()> {
    STATE
        .set(Mutex::new(DefaultWindowState::new(60)))
        .map_err(|err| eyre!("state init: {:#?}", err))
}

static STATE: OnceLock<Mutex<DefaultWindowState>> = OnceLock::new();

#[smartmodule(filter_map)]
pub fn filter_map(record: &Record) -> Result<Option<(Option<RecordData>, RecordData)>> {

    let mqtt: MQTTEvent = serde_json::from_slice(record.value.as_ref())?;
    if let Some(vp) = mqtt.payload.VP {
        if vp.spd.is_some() {

            let mut stats = STATE.get().unwrap().lock().unwrap();
            if let Some(window_completed) = stats.add(vp.clone()) {

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
*/
