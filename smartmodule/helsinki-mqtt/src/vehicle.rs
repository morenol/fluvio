use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use anyhow::Result;

use fluvio_smartmodule_window::{
    window::{TumblingWindow, Value, WindowStates},
    mean::RollingMean,
};

type Key = u16;
pub type DefaultWindowState = TumblingWindow<VehiclePosition, VehicleStatistics>;

/// business logic
#[derive(Debug, Deserialize)]
pub struct MQTTEvent {
    pub mqtt_topic: String,
    pub payload: Payload,
}

#[derive(Debug, Deserialize)]
pub struct Payload {
    pub VP: Option<VehiclePosition>,
}

/// city of Helinski metro event
/// https://digitransit.fi/en/developers/apis/4-realtime-api/vehicle-positions/high-frequency-positioning/
///
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct VehiclePosition {
    pub desi: String,       // Route number visible to passengers.
    pub lat: Option<f32>,   // WGS 84 latitude in degrees. null if location is unavailable.
    pub long: Option<f32>,  // WGS 84 longitude in degrees.null if location is unavailable.
    pub dir: String, // Route direction of the trip. After type conversion matches direction_id in GTFS and the topic. Either "1" or "2".
    pub oper: u16, // Unique ID of the operator running the trip (i.e. this value can be different than the operator ID in the topic, for example if the service has been subcontracted to another operator). The unique ID does not have prefix zeroes here.
    pub veh: Key, // Vehicle number that can be seen painted on the side of the vehicle, often next to the front door. Different operators may use overlapping vehicle numbers. Matches vehicle_number in the topic except without the prefix zeroes.
    pub tst: DateTime<Utc>, // UTC timestamp with millisecond precision from the vehicle in ISO 8601 format (yyyy-MM-dd'T'HH:mm:ss.SSSZ).
    pub tsi: u64,           // Unix time in seconds from the vehicle.
    pub spd: Option<f32>,   // Speed of the vehicle, in meters per second (m/s).
    pub hdg: Option<u16>, // Heading of the vehicle, in degrees (⁰) starting clockwise from geographic north. Valid values are on the closed interval [0, 360].
    pub acc: Option<f32>, // Acceleration (m/s^2), calculated from the speed on this and the previous message. Negative values indicate that the speed of the vehicle is decreasing.
    pub dl: Option<i32>, // Offset from the scheduled timetable in seconds (s). Negative values indicate lagging behind the schedule, positive values running ahead of schedule.
    pub odo: Option<u32>, // The odometer reading in meters (m) since the start of the trip. Currently the values not very reliable.
    pub drst: Option<u32>, // Door status. 0 if all the doors are closed. 1 if any of the doors are open.
    pub oday: String, // Operating day of the trip. The exact time when an operating day ends depends on the route. For most routes, the operating day ends at 4:30 AM on the next day. In that case, for example, the final moment of the operating day "2018-04-05" would be at 2018-04-06T04:30 local time.
    pub jrn: Option<u32>, // Internal journey descriptor, not meant to be useful for external use.
    pub line: Option<u16>, // Internal line descriptor, not meant to be useful for external use.
    pub start: String, // Scheduled start time of the trip, i.e. the scheduled departure time from the first stop of the trip. The format follows HH:mm in 24-hour local time, not the 30-hour overlapping operating days present in GTFS. Matches start_time in the topic.
    pub loc: String, // Location source, either GPS, ODO, MAN, DR or N/A. GPS - location is received from GPS  ODO - location is calculated based on odometer value   MAN - location is specified manually   DR - location is calculated using dead reckoning (used in tunnels and other locations without GPS signal) N/A - location is unavailable
    pub stop: Option<u32>, // ID of the stop related to the event (e.g. ID of the stop where the vehicle departed from in case of dep event or the stop where the vehicle currently is in case of vp event).null if the event is not related to any stop.
    pub route: String, // ID of the route the vehicle is currently running on. Matches route_id in the topic.
    pub occu: u16, // Integer describing passenger occupancy level of the vehicle. Valid values are on interval [0, 100]. Currently passenger occupancy level is only available for Suomenlinna ferries as a proof-of-concept. The value will be available shortly after departure when the ferry operator has registered passenger count for the journey.For other vehicles, currently only values used are 0 (= vehicle has space and is accepting passengers) and 100 (= vehicle is full and might not accept passengers)
}

impl Value for VehiclePosition {
    type Key = Key;

    fn key(&self) -> Result<Option<Self::Key>> {
        Ok(Some(self.veh))
    }

    fn time(&self) -> Option<fluvio_smartmodule_window::time::FluvioTime> {
        Some(self.tst).map(|time| time.into())
    }
}

#[derive(Debug, Serialize)]
pub struct VehicleStatistics {
    pub vehicle: u16,
    pub avg_speed: RollingMean,
}

impl Default for VehicleStatistics {
    fn default() -> Self {
        Self {
            vehicle: 22,
            avg_speed: RollingMean::default(),
        }
    }
}

impl WindowStates<VehiclePosition> for VehicleStatistics {
    fn add(&mut self, _key: Key, value: VehiclePosition) {
        self.avg_speed.add(value.spd.unwrap_or_default() as f64);
    }

    fn new_with_key(key: Key) -> Self {
        Self {
            vehicle: key,
            avg_speed: RollingMean::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;

    use chrono::{DateTime, FixedOffset};

    use super::MQTTEvent;
    #[test]
    fn json_parse() {
        let bytes = fs::read("test/test.json").expect("read file");
        let mqtt: MQTTEvent = serde_json::from_slice(&bytes).expect("parse json");
        let event = mqtt.payload.VP.unwrap();
        assert_eq!(event.veh, 116);
        assert_eq!(event.lat, Some(60.178622));
        assert_eq!(event.long, Some(24.950366));
        assert_eq!(
            event.tst,
            DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.081Z", "%+")
                .expect("datetime parser error")
        );
    }

    #[test]
    fn json_parse_2() {
        let bytes = fs::read("test/test2.json").expect("read file");
        let mqtt: MQTTEvent = serde_json::from_slice(&bytes).expect("parse json");
        let event = mqtt.payload.VP.unwrap();
        assert_eq!(event.veh, 1071);
        assert_eq!(event.lat, Some(60.174404));
        assert_eq!(event.long, Some(24.94097));
        assert_eq!(
            event.tst,
            DateTime::<FixedOffset>::parse_from_str("2023-07-11T23:27:14.554Z", "%+")
                .expect("datetime parser error")
        );
    }
}
