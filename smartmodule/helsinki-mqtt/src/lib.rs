mod vehicle;
//mod host;

use fluvio_smartmodule::{smartmodule, Record, Result, eyre, RecordData};
use vehicle::{MQTTEvent, DefaultWindowState};

#[smartmodule(materialize)]
pub fn filter_map(
    record: &Record,
    stats: &mut DefaultWindowState,
) -> Result<Option<(Option<RecordData>, RecordData)>> {
    let mqtt: MQTTEvent = serde_json::from_slice(record.value.as_ref())?;
    if let Some(vp) = mqtt.payload.VP {
        if vp.spd.is_some() {
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
                Ok(None)
            }
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}
