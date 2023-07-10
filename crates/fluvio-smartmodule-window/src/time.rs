use std::{time::Duration, cmp::Ordering};

use anyhow::Result;
use chrono::{DateTime, FixedOffset, NaiveDateTime};

pub type UTC = chrono::DateTime<chrono::Utc>;

/// Efficient time represention for use in Fluvio
/// Make following assumption, this will fit into maximum time fit into microseconds since EPOCH
/// and UTC timezone
#[derive(Debug, Clone, Copy, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct FluvioTime(i64);

impl From<UTC> for FluvioTime {
    fn from(timestamp: UTC) -> Self {
        Self(timestamp.timestamp_micros())
    }
}

const MICRO_PER_SEC: i64 = 1000 * 1000;

impl FluvioTime {
    /// parse time with timezone.
    pub fn parse_from_str(timestamp: &str) -> Result<Self> {
        let time: UTC = DateTime::<FixedOffset>::parse_from_str(timestamp, "%+")
            .map_err(|err| anyhow::anyhow!("time parse error: {}", err))?
            .into();
        Ok(Self(time.timestamp_micros()))
    }

    /// new base time to nearest seconds
    pub fn align_seconds(&self, seconds: u32) -> Self {
        Self(self.0 - (self.0 % (MICRO_PER_SEC * seconds as i64)))
    }

    pub fn timestamp_micros(&self) -> i64 {
        self.0
    }

    /// convert back to UtC
    pub fn as_utc(&self) -> Option<UTC> {
        NaiveDateTime::from_timestamp_micros(self.0).map(|naive| naive.and_utc())
    }
}

#[cfg(test)]
mod tests {

    use chrono::{DateTime, FixedOffset};

    use super::*;

    #[test]
    fn test_conversion() {
        let t = FluvioTime::parse_from_str("2023-06-22T19:45:22.033Z").expect("parse");
        assert_eq!(
            t.as_utc().unwrap(),
            DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.033Z", "%+")
                .expect("parse")
        );
    }

    #[test]
    fn test_nearest() {
        let t = FluvioTime::parse_from_str("2023-06-22T19:45:22.033Z").expect("parse");
        assert_eq!(
            t.align_seconds(1),
            FluvioTime::parse_from_str("2023-06-22T19:45:22.000Z").expect("parse")
        );

        assert_eq!(
            t.align_seconds(5),
            FluvioTime::parse_from_str("2023-06-22T19:45:20.000Z").expect("parse")
        );
        assert_eq!(
            t.align_seconds(60),
            FluvioTime::parse_from_str("2023-06-22T19:45:00.000Z").expect("parse")
        );
        assert_eq!(
            t.align_seconds(300),
            FluvioTime::parse_from_str("2023-06-22T19:45:00.000Z").expect("parse")
        ); // 5min

        let t2 = FluvioTime::parse_from_str("2023-06-22T19:46:22.033Z").expect("parse");
        assert_eq!(
            t2.align_seconds(5),
            FluvioTime::parse_from_str("2023-06-22T19:46:20.000Z").expect("parse")
        );
        assert_eq!(
            t2.align_seconds(300),
            FluvioTime::parse_from_str("2023-06-22T19:45:00.000Z").expect("parse")
        );
        assert_eq!(
            t2.align_seconds(3600),
            FluvioTime::parse_from_str("2023-06-22T19:00:00.000Z").expect("parse")
        ); //1 hr
    }
}
