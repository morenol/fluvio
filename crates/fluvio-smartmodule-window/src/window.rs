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

    fn add(&mut self, key: V::Key, value: V);
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
    duration_in_micros: i64,
    state: HashMap<V::Key, S>,
}

impl<V, S> TimeWindow<V, S>
where
    V: Value,
    S: WindowStates<V>,
    V::Key: PartialEq + Eq + Hash + Clone,
{
    pub fn new(start: FluvioTime, duration_in_secs: u16) -> Self {
        Self {
            start,
            duration_in_micros: duration_in_secs as i64 * MICRO_PER_SEC,
            state: HashMap::new(),
        }
    }

    pub fn duration_in_micros(&self) -> i64 {
        self.duration_in_micros
    }

    /// try to add value to window
    /// if value can't fit into window, return back
    pub fn add(&mut self, time: &FluvioTime, value: V) -> Option<V> {

        if time.timestamp_micros() > self.start.timestamp_micros() + self.duration_in_micros{
           return Some(value)
        } 

        let key = value.key();
        if let Some(state) = self.state.get_mut(&key) {
            state.add(key.to_owned(), value);
        } else {
            self.state.insert(key.clone(), S::new_with_key(key.clone()));
            if let Some(state) = self.state.get_mut(&key) {
                state.add(key.to_owned(), value);
            }
        }
        None
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
    _future_windows: Vec<TimeWindow<V, S>>,
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
            _future_windows: vec![],
        }
    }

    /// add new value based on time
    /// if time is not found, it will be created
    /// if current window is expired, previous will be returned
    pub fn add(&mut self, value: V) -> Option<TimeWindow<V,S>> {
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
            None
        } else {
            let mut current_window = TimeWindow::new(window_base, self.window_size_sec);
            current_window.add(&event_time, value);
            self.current_window = Some(current_window);
            None
            
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
    use crate::window::MICRO_PER_SEC;

    use super::{TumblingWindow, TimeWindow};
    use super::{TimeSortedStates,Value,WindowStates,WindowState};

    type KEY = u16;

    const VEH1: KEY = 22;
    const VEH2: KEY = 33;

    #[derive(Debug, Default,Clone,PartialEq)]
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

        fn add(&mut self, _key: KEY, value: TestValue) {
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

    type DefaultTimeWindow = TimeWindow<TestValue, TestState>;
    #[test]
    fn test_window_add() {
        let mut w = DefaultTimeWindow::new(FluvioTime::parse_from_str("2023-06-22T19:45:20.000Z").unwrap(),10);

        let v1 = TestValue {
            speed: 3.2,
            vehicle: VEH1,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.132Z", "%+")
                .expect("parse")
                .into(),
        };


        assert!(w.add(&v1.time(), v1).is_none());

        let v2 = TestValue {
            speed: 3.2,
            vehicle: VEH1,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:50.132Z", "%+")
                .expect("parse")
                .into(),
        };

        let out = w.add(&v2.time(), v2.clone());
        assert!(out.is_some());
        assert_eq!(out.unwrap(),v2);

    }



    #[test]
    fn test_add_new_value_to_empty_window() {

        let mut window = DefaultSortedWindow::new(10);
        assert!(window.current_window.is_none());

        let v1 = TestValue {
            speed: 3.2,
            vehicle: VEH1,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.132Z", "%+")
                .expect("parse")
                .into(),
        };


        window.add(v1);
        assert!(window.current_window.is_some());
        let current_window = window.current_window.as_ref().unwrap();
        assert_eq!(current_window.start, FluvioTime::parse_from_str("2023-06-22T19:45:20.000Z").unwrap());
        assert_eq!(current_window.duration_in_micros(), 10* MICRO_PER_SEC);
        assert_eq!(current_window.state.len(), 1);
        assert_eq!(current_window.state.get(&VEH1).unwrap().speed.mean(), 3.2);

    }
}
