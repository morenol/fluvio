use std::time::Duration;
use std::{marker::PhantomData, collections::HashMap};
use std::hash::{Hash};

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

    pub fn nearest(&self, duration: &Duration) -> Self {
        Self(self.0 - (self.0 % duration.as_nanos() as i64))
    }
}

pub trait Value {
    type Key;

    fn key(&self) -> &Self::Key;
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
    state: HashMap<V::Key, S>,
}

impl<V, S> TimeWindow<V, S>
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
    window_size_sec: u16, // window size in seconds
    current_window: Option<TimeWindow<V, S>>,
    future_windows: Vec<TimeWindow<V, S>>,
}

impl<V, S> TimeSortedStates<V, S>
where
    V: Value,
    S: WindowStates<V>,
    V::Key: PartialEq + Eq + Hash + Clone,
{

    pub fn new(window_size_sec: u16) -> Self {
        Self {
            window_size_sec,
            current_window: None,
            future_windows: vec![],
        }
    }

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

mod stats_lock_free {}

#[cfg(test)]
mod test {
    use chrono::{DateTime, Utc, FixedOffset};

    use crate::mean::RollingMean;
    use crate::time::FluvioTime;

    use super::TumblingWindow;
    use super::{TimeSortedStates,Value,WindowStates,WindowState};

    type KEY = u16;

    const VEH1: KEY = 22;
    const VEH2: KEY = 33;

    #[derive(Debug, Default)]
    struct TestValue {
        speed: f64,
        vehicle: KEY,
        time: DateTime<Utc>,
    }

    impl Value for TestValue {
        type Key = KEY;

        fn key(&self) -> &Self::Key {
            &self.vehicle
        }

        fn time(&self) -> FluvioTime {
            self.time.into()
        }
    }

    #[derive(Debug, Default)]
    struct TestState {
        key: KEY,
        speed: RollingMean,
    }

    impl WindowStates<TestValue> for TestState {
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

    impl WindowState<KEY, TestValue> for TestState {
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
            vehicle: VEH1,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.002Z", "%+")
                .expect("parse")
                .into(),
        };

        window.add(22, &v1);

        let v2 = TestValue {
            speed: 4.2,
            vehicle: VEH2,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.033Z", "%+")
                .expect("parse")
                .into(),
        };

        window.add(22, &v2);

        assert_eq!(window.get_state(&22).unwrap().speed.mean(), 3.7);
    }

    type DefaultSortedWindow = TimeSortedStates<TestValue, TestState>;

    #[test]
    fn test_add_new_value_to_empty_window() {

        let mut window = DefaultSortedWindow::new(10);

    }
}
