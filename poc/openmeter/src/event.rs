use std::ops::{Deref, DerefMut};

use cloudevents::{Event, AttributesReader};
use fluvio_smartmodule_window::{window::Value, time::FluvioTime};



pub struct OpenMeterEvent {
    event: Event,
    key: Option<String>
}

impl OpenMeterEvent {
    pub fn new(event: Event, key: String) -> Self {
        Self { event, key: Some(key)}
    }
}

impl From<Event> for OpenMeterEvent {
    fn from(event: Event) -> Self {
        Self { event, key: None }
    }
}

impl Deref for OpenMeterEvent {
    type Target = Event;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

impl DerefMut for OpenMeterEvent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.event
    }
}

impl Value for OpenMeterEvent {
    type Key = String;

    fn key(&self) -> Option<&Self::Key> {
      //  &self.key
      None
    }

    fn time(&self) -> Option<FluvioTime> {
        self.event.time().map(|time| time.into())
    }
}

#[cfg(test)]
mod test {
    use std::fs;

    use chrono::{DateTime, FixedOffset, Utc};
    use cloudevents::{Event, AttributesReader};

    #[test]
    fn json_parse() {
        let bytes = fs::read("test/event.json").expect("read file");
        let event: Event = serde_json::from_slice(&bytes).expect("parse json");
        assert_eq!(event.ty(), "api-calls");
        assert_eq!(event.subject(), Some("customer-1"));
        let test_time: DateTime<Utc> =
            DateTime::<FixedOffset>::parse_from_str("2023-01-01T00:00:00.001Z", "%+")
                .expect("datetime parser error")
                .into();
        assert_eq!(event.time().unwrap(), &test_time);
    }
}
