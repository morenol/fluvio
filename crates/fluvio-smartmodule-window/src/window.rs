use std::time::Duration;
use std::{marker::PhantomData, collections::HashMap};
use std::hash::{Hash};

//pub use util::{AtomicF64,RollingMean};
pub use stats::RollingMean;

use crate::time::{UTC, FluvioTime};


type TimeStamp = i64;

/// Fluvio timestmap representing nanoseconds since UNIX epoch
pub struct FluvioTimeStamp(i64);

impl From<i64> for FluvioTimeStamp {
    fn from(timestamp: i64) -> Self {
        Self(timestamp)
    }
}

const MILLI_PER_SEC: i64 = 1_000;
const MICRO_PER_SEC: i64 = 1_000_000;
const NANOS_PER_SEC: i64 = 1_000_000_000;

impl FluvioTimeStamp {
    pub fn new(timestamp: i64) -> Self {
        Self(timestamp)
    }

    pub fn nearest(&self,duration: &Duration) -> Self {
        Self(self.0 - (self.0 % duration.as_nanos() as i64))
    }
}

pub trait Value {
    type Key;

    fn key(&self) -> Self::Key;
    fn time(&self) -> FluvioTime;
}

pub trait WindowStates<V: Value> {
    fn new_with_key(key: V::Key) -> Self;

    fn add(&mut self, key: &V::Key, value: &V);
}

pub trait WindowState<K, V> {
    fn new_with_key(key: K) -> Self;
    fn add(&mut self, key: &K, value: &V);
}

#[derive(Debug, Default)]
pub struct TumblingWindow<K, V, S> {
    phantom: PhantomData<K>,
    phantom2: PhantomData<V>,
    store: HashMap<K, S>,
    watermark: WaterMark,
}

impl<K, V, S> TumblingWindow<K, V, S>
where
    S: Default + WindowState<K, V>,
    K: PartialEq + Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
            phantom2: PhantomData,
            store: HashMap::new(),
            watermark: WaterMark::new(),
        }
    }

    /// add new value to state
    pub fn add(&mut self, key: K, value: &V) {
        if let Some(state) = self.store.get_mut(&key) {
            state.add(&key, value);
        } else {
            self.store.insert(key.clone(), S::new_with_key(key.clone()));
            if let Some(state) = self.store.get_mut(&key) {
                state.add(&key, value);
            }
        }
    }

    pub fn get_state(&self, key: &K) -> Option<&S> {
        self.store.get(key)
    }

    pub fn summary(&self) -> Vec<&S> {
        self.store.values().collect()
    }
}

pub struct TimeWindow<V, S>
where
    V: Value,
    S: WindowStates<V>,
{    
    start: FluvioTime,
    duration: Duration,
    state: HashMap<V::Key, S>
}

impl <V,S>  TimeWindow<V,S> 
where
    V: Value,
    S: WindowStates<V>,
    V::Key: PartialEq + Eq + Hash + Clone,
{
    pub fn new(start: FluvioTime, duration_in_seconds: u16) -> Self {
        Self {
            start,
            duration: Duration::from_secs(duration_in_seconds as u64),
            state: HashMap::new(),
        }
    }

    pub fn add(&mut self, value: &V) {
        let key = value.key();
        if let Some(state) = self.state.get_mut(&key) {
            state.add(&key, value);
        } else {
            self.state.insert(key.clone(), S::new_with_key(key.clone()));
            if let Some(state) = self.state.get_mut(&key) {
                state.add(&key, value);
            }
        }
    }

}

/// split state by time
pub struct TimeSortedStates<V, S>
where
    V: Value,
    S: WindowStates<V>,
{
    window_size_sec: u16,      // window size in seconds
    current_window: Option<TimeWindow<V, S>>,
    future_windows: Vec<TimeWindow<V, S>>,
}

impl<V, S> TimeSortedStates<V, S>
where
    V: Value,
    S: WindowStates<V>,
    V::Key: PartialEq + Eq + Hash + Clone,
{
    /// add new value based on time
    /// if time is not found, it will be created
    pub fn add(&mut self, value: &V) {

        let event_time = value.time();
        let window_base = event_time.align_seconds(self.window_size_sec as u32);

        if let Some(current_window) = &mut self.current_window {
            /* 
            if value.timestamp() < current_window.start {
                // we need to create new window
                let window = value.timestamp() - (value.timestamp() % self.window);       // round to nearest second
                let new_window = TimeWindow::new(window, self.window);
                self.future_windows.push(new_window);
            } else {
                // we are still in current window
                current_window.add(value);
            }*/
        } else {
            let mut current_window = TimeWindow::new(window_base, self.window_size_sec);
            current_window.add(value);
            self.current_window = Some(current_window);
            /* 
            // we need to create new window
            let window = value.timestamp() - (value.timestamp() % self.window);       // round to nearest second
            let new_window = TimeWindow::new(window, self.window);
            self.future_windows.push(new_window);
            */
        }
    }
}

/// watermark
#[derive(Debug, Default)]
pub struct WaterMark {}

impl WaterMark {
    pub fn new() -> Self {
        Self {}
    }
}

// lock free stats

mod stats_lock_free {
    use std::{
        sync::atomic::{AtomicU64, Ordering, AtomicU32},
        ops::{Deref, DerefMut},
    };

    #[derive(Debug, Default)]
    pub struct AtomicF64(AtomicU64);

    impl Deref for AtomicF64 {
        type Target = AtomicU64;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl DerefMut for AtomicF64 {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl AtomicF64 {
        pub fn new(value: f64) -> Self {
            let as_u64 = value.to_bits();
            Self(AtomicU64::new(as_u64))
        }

        pub fn store(&self, value: f64) {
            let as_u64 = value.to_bits();
            self.0.store(as_u64, Ordering::SeqCst)
        }

        pub fn load(&self) -> f64 {
            let as_u64 = self.0.load(Ordering::SeqCst);
            f64::from_bits(as_u64)
        }
    }

    #[cfg(feature = "use_serde")]
    mod serde_util {

        use std::fmt;

        use serde::{
            Serialize, Deserialize, Serializer, Deserializer,
            de::{Visitor, self},
        };

        use super::*;

        struct AtomicF64Visitor;

        impl Serialize for AtomicF64 {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_f64(self.load())
            }
        }

        impl<'de> Visitor<'de> for AtomicF64Visitor {
            type Value = AtomicF64;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an float between -2^31 and 2^31")
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                use std::f64;
                if value >= f64::from(f64::MIN) && value <= f64::from(f64::MAX) {
                    Ok(AtomicF64::new(value))
                } else {
                    Err(E::custom(format!("f64 out of range: {}", value)))
                }
            }
        }

        impl<'de> Deserialize<'de> for AtomicF64 {
            fn deserialize<D>(deserializer: D) -> Result<AtomicF64, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_f64(AtomicF64Visitor)
            }
        }
    }

    #[derive(Debug, Default)]
    #[cfg_attr(feature = "use_serde", derive(serde::Serialize))]
    pub struct LockFreeRollingMean {
        #[cfg_attr(feature = "use_serde", serde(skip))]
        count: AtomicU32,
        mean: AtomicF64,
    }

    impl LockFreeRollingMean {
        /// add to sample
        pub fn add(&self, value: f64) {
            let prev_mean = self.mean.load();
            let new_count = self.count.load(Ordering::SeqCst) + 1;
            let new_mean = prev_mean + (value - prev_mean) / (new_count as f64);
            self.mean.store(new_mean);
            self.count.store(new_count, Ordering::SeqCst);
        }

        pub fn mean(&self) -> f64 {
            self.mean.load()
        }
    }

    #[cfg(test)]
    mod test {

        use super::*;

        #[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
        struct Sample {
            speed: AtomicF64,
        }

        #[test]
        fn rolling_mean() {
            let rm = LockFreeRollingMean::default();
            rm.add(3.2);
            assert_eq!(rm.mean(), 3.2);
            rm.add(4.2);
            assert_eq!(rm.mean(), 3.7);
        }

        #[cfg(feature = "use_serde")]
        mod test_serde {

            use serde::{Serialize, Deserialize};

            use super::*;

            #[test]
            fn test_f64_serialize() {
                let test = Sample {
                    speed: AtomicF64::new(3.2),
                };
                let json = serde_json::to_string(&test).expect("serialize");
                assert_eq!(json, r#"{"speed":3.2}"#);
            }

            #[test]
            fn test_f64_de_serialize() {
                let input_str = r#"{"speed":9.13}"#;
                let test: Sample = serde_json::from_str(input_str).expect("serialize");
                assert_eq!(test.speed.load(), 9.13);
            }
        }
    }
}

mod stats {

    #[derive(Debug, Default)]
    #[cfg_attr(feature = "use_serde", derive(serde::Serialize))]
    pub struct RollingMean {
        #[cfg_attr(feature = "use_serde", serde(skip))]
        count: u32,
        mean: f64,
    }

    impl RollingMean {
        /// add to sample
        pub fn add(&mut self, value: f64) {
            let prev_mean = self.mean;
            let new_count = self.count + 1;
            let new_mean = prev_mean + (value - prev_mean) / (new_count as f64);
            self.mean = new_mean;
            self.count = new_count;
        }

        pub fn mean(&self) -> f64 {
            self.mean
        }
    }

    #[cfg(test)]
    mod test {

        use super::*;

        #[test]
        fn rolling_mean() {
            let mut rm = RollingMean::default();
            rm.add(3.2);
            assert_eq!(rm.mean(), 3.2);
            rm.add(4.2);
            assert_eq!(rm.mean(), 3.7);
        }
    }
}

#[cfg(test)]
mod test {
    use chrono::{DateTime, Utc, FixedOffset};

    use super::RollingMean;
    use super::TumblingWindow;

    type KEY = u16;

    #[derive(Debug, Default)]
    struct TestValue {
        speed: f64,
        time: DateTime<Utc>,
    }

    #[derive(Debug, Default)]
    struct TestState {
        key: KEY,
        speed: RollingMean,
    }

    impl super::WindowState<KEY, TestValue> for TestState {
        fn new_with_key(key: KEY) -> Self {
            Self {
                key,
                speed: RollingMean::default(),
            }
        }

        fn add(&mut self, _key: &KEY, value: &TestValue) {
            self.speed.add(value.speed);
        }
    }

    type DefaultTumblingWindow = TumblingWindow<KEY, TestValue, TestState>;

    #[test]
    fn test_add() {
        let mut window = DefaultTumblingWindow::new();

        let v1 = TestValue {
            speed: 3.2,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.002Z", "%+")
                .expect("parse")
                .into(),
        };

        window.add(22, &v1);

        let v2 = TestValue {
            speed: 4.2,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.033Z", "%+")
                .expect("parse")
                .into(),
        };

        window.add(22, &v2);

        assert_eq!(window.get_state(&22).unwrap().speed.mean(), 3.7);
    }
}
