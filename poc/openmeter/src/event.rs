use std::ops::{Deref, DerefMut};

use cloudevents::{Event, AttributesReader, Data};
use serde::Serialize;
use serde_json::Value as JsonValue;
use anyhow::Result;

use fluvio_smartmodule_window::{
    window::{Value, WindowStates, TumblingWindow},
    time::FluvioTime,
    mean::RollingSum,
};

pub type DefaultTumblingWindow = TumblingWindow<OpenMeterEvent, MeterStatistics>;

#[derive(Debug)]
pub struct OpenMeterEvent {
    event: Event,
}

impl OpenMeterEvent {
    pub fn new(event: Event) -> Self {
        Self { event }
    }

    pub fn json_data(&self) -> Option<&JsonValue> {
        match self.event.data() {
            Some(ref data) => match data {
                Data::Json(json) => Some(json),
                _ => None,
            },
            None => None,
        }
    }

    /// get data value from json
    pub fn data_value(&self, path: &str) -> Option<&JsonValue> {
        if let Some(json_value) = self.json_data() {
            json_value.get(path)
        } else {
            None
        }
    }
}

impl From<Event> for OpenMeterEvent {
    fn from(event: Event) -> Self {
        Self { event }
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
    type Selector = String;
    type KeyValue = String;
    type Value = u64;

    fn key(&self, selector: &String) -> Result<Option<Self::KeyValue>> {
        if let Some(json_value) = self.json_data() {
            if let Some(path) = json_value.get(selector) {
                return Ok(path.as_str().map(|s| s.to_owned()));
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn time(&self) -> Option<FluvioTime> {
        self.event.time().map(|time| time.into())
    }

    fn value(&self, selector: &Self::Selector) -> Result<Option<Self::Value>> {
        todo!()
    }
}

type MeterSum = RollingSum<i64>;

#[derive(Debug, Serialize, Default)]
pub struct MeterStatistics {
    pub subject: String,
    pub property: String,
    pub sum: MeterSum,
}

impl WindowStates<OpenMeterEvent> for MeterStatistics {
    fn add(&mut self, _key: String, value: OpenMeterEvent) {
        self.sum.add(
            value
                .data_value(&self.property)
                .map(|v| v.as_i64())
                .flatten()
                .unwrap_or_default(),
        );
    }

    fn new_with_key(key: <OpenMeterEvent as Value>::KeyValue) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::fs;

    use chrono::{DateTime, FixedOffset, Utc};
    use cloudevents::{Event, AttributesReader};
    use fluvio_smartmodule_window::window::Value;

    use crate::event::OpenMeterEvent;

    use super::DefaultTumblingWindow;

    fn read_event(path: &str) -> Event {
        let bytes = fs::read(path).expect("read file");
        serde_json::from_slice(&bytes).expect("parse json")
    }

    #[test]
    fn json_parse() {
        let event: Event = read_event("test/event.json");
        assert_eq!(event.ty(), "api-calls");
        assert_eq!(event.subject(), Some("customer-1"));
        let test_time: DateTime<Utc> =
            DateTime::<FixedOffset>::parse_from_str("2023-01-01T00:00:00.001Z", "%+")
                .expect("datetime parser error")
                .into();
        assert_eq!(event.time().unwrap(), &test_time);

        let m = OpenMeterEvent::new(event);
        assert!(m.json_data().is_some());
        assert_eq!(m.key(&"path".to_owned()).expect("key").unwrap(), "/hello");
        assert_eq!(m.time().expect("time"), test_time.into());
    }

    #[test]
    fn test_add() {
        let mut window = DefaultTumblingWindow::new(3600, "order".to_owned());

        let event1: OpenMeterEvent = read_event("test/test1.json").into();
        assert!(window.add(event1).expect("add").is_none());

        let event2: OpenMeterEvent = read_event("test/test2.json").into();
        assert!(window.add(event2).expect("add").is_none());

        let current_window = window.current_window().expect("current_window");
        let stat = current_window
            .get_state(&"espresso".to_owned())
            .expect("espresso");

        assert_eq!(stat.sum.sum(), 2);

        // let event3: OpenMeterEvent = read_event("test/test3.json").into();
        // assert!(window.add(event3).expect("add").is_some());
    }
}
