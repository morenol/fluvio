use std::io::{Error, ErrorKind};

use chrono::{DateTime, Utc, NaiveDateTime};
use fluvio_protocol::bytes::BufMut;
use fluvio_protocol::{Encoder, Decoder};

/// input to SmartModule Window
#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct SmartModuleWindowInput {
    pub starting_time: FluvioDateTime,
    pub end_time: FluvioDateTime,
}

/// wrapper for chrono DateTime
#[derive(Debug, Default, Clone)]
pub struct FluvioDateTime(DateTime<Utc>);

impl From<DateTime<Utc>> for FluvioDateTime {
    fn from(time: DateTime<Utc>) -> Self {
        Self(time)
    }
}

impl Encoder for FluvioDateTime {
    fn write_size(&self, _version: i16) -> usize {
        8
    }

    fn encode<T>(&self, dest: &mut T, version: i16) -> Result<(), Error>
    where
        T: BufMut,
    {
        self.0.timestamp_micros().encode(dest, version)
    }
}

impl Decoder for FluvioDateTime {
    fn decode<T>(&mut self, src: &mut T, _version: fluvio_protocol::Version) -> Result<(), Error>
    where
        T: fluvio_protocol::bytes::Buf,
    {
        let mut ts: i64 = 0;
        ts.decode(src, _version)?;
        if let Some(time) = NaiveDateTime::from_timestamp_micros(ts) {
            self.0 = time.and_utc();
            Ok(())
        } else {
            Err(Error::new(ErrorKind::InvalidData, "invalid timestamp"))
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDateTime, FixedOffset};

    use super::*;

    // convert date time to timestamp and back
    #[test]
    fn test_date_time_serialization_using_timestamp() {
        use chrono::{DateTime, FixedOffset};

        let time = DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.081Z", "%+")
            .expect("datetime parser error");

        let ts = time.timestamp_micros();

        let naive_time = NaiveDateTime::from_timestamp_micros(ts).unwrap();
        assert_eq!(time, naive_time.and_utc());
    }

    #[test]
    fn test_encode_decode_time() {
        let time = FluvioDateTime(
            DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.081Z", "%+")
                .expect("datetime parser error")
                .into(),
        );
        let mut bytes = vec![];
        time.encode(&mut bytes, 0).expect("encode");
        let mut time2 = FluvioDateTime::default();
        time2.decode(&mut bytes.as_slice(), 0).expect("decode");
        assert_eq!(time.0, time2.0);
    }
}
