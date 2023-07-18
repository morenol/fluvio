use std::time::Duration;
use std::fmt::Debug;
use std::{collections::HashMap};
use std::hash::{Hash};

use anyhow::Result;
use derive_builder::Builder;

use crate::time::{FluvioTime, UTC};

/// Fluvio timestmap representing nanoseconds since UNIX epoch
pub struct FluvioTimeStamp(i64);

impl From<i64> for FluvioTimeStamp {
    fn from(timestamp: i64) -> Self {
        Self(timestamp)
    }
}

const MICRO_PER_SEC: i64 = 1_000_000;

impl FluvioTimeStamp {
    pub fn new(timestamp: i64) -> Self {
        Self(timestamp)
    }

    pub fn nearest(&self, duration: &Duration) -> Self {
        Self(self.0 - (self.0 % duration.as_nanos() as i64))
    }
}

pub trait Selector {}

impl Selector for String {}

pub trait Value {
    type KeyValue;
    type Value;

    type Selector: Selector;

    /// get key
    fn key(&self, selector: &Self::Selector) -> Result<Option<Self::KeyValue>>;
    fn value(&self, selector: &Self::Selector) -> Result<Option<Self::Value>>;

    fn time(&self) -> Option<FluvioTime>;
}

pub trait WindowStates<V: Value> {
    fn new_with_key(key: V::KeyValue) -> Self;

    fn add(&mut self, key: V::KeyValue, value: V::Value);
}

#[cfg_attr(feature = "use_serde", derive(serde::Serialize))]
pub struct WindowSummary<V, S>
where
    V: Value,
    S: WindowStates<V>,
{
    start: UTC,
    end: UTC,
    values: Vec<S>,
    #[cfg_attr(feature = "use_serde", serde(skip))]
    phantom: std::marker::PhantomData<V>,
}

impl<V, S> WindowSummary<V, S>
where
    V: Value,
    S: WindowStates<V>,
{
    pub fn start(&self) -> &UTC {
        &self.start
    }

    pub fn end(&self) -> &UTC {
        &self.end
    }

    pub fn values(&self) -> &Vec<S> {
        &self.values
    }
}

#[derive(Debug)]
pub struct TimeWindow<V, S>
where
    V: Value,
    S: WindowStates<V>,
{
    start: FluvioTime,
    duration_in_micros: i64,
    state: HashMap<V::KeyValue, S>,
}

impl<V, S> TimeWindow<V, S>
where
    V: Value,
    S: WindowStates<V>,
    V::KeyValue: PartialEq + Eq + Hash + Clone,
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
    pub fn add(
        &mut self,
        time: &FluvioTime,
        key: V::KeyValue,
        value: V::Value,
    ) -> Option<V::Value> {
        if time.timestamp_micros() > self.start.timestamp_micros() + self.duration_in_micros {
            return Some(value);
        }

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

    pub fn get_state(&self, key: &V::KeyValue) -> Option<&S> {
        self.state.get(key)
    }

    pub fn summary(self) -> WindowSummary<V, S> {
        WindowSummary {
            start: <FluvioTime as Into<Option<UTC>>>::into(self.start).unwrap(),
            end: <FluvioTime as Into<Option<UTC>>>::into(
                self.start.add_micro_seconds(self.duration_in_micros),
            )
            .unwrap(),
            values: self.state.into_values().collect(),
            phantom: std::marker::PhantomData,
        }
    }
}

#[derive(Debug, Builder)]
#[builder(build_fn(private, name = "build_impl"), pattern = "owned")]
pub struct WindowConfig<V>
where
    V: Value + Debug,
    V::KeyValue: Debug,
    V::Selector: Clone + Debug,
{
    #[builder(setter(into), default = "10")]
    window_size_sec: u16, // window size in seconds
    #[builder(setter(into))]
    value_selector: V::Selector,
    #[builder(setter(into))]
    key_selector: V::Selector,
}

impl<V> WindowConfigBuilder<V>
where
    V: Value + Debug + Clone,
    V::KeyValue: Debug,
    V::Selector: Clone + Debug,
{
    pub fn build<S: WindowStates<V>>(self) -> Result<TumblingWindow<V, S>> {
        let config = self.build_impl()?;

        Ok(TumblingWindow {
            config,
            _future_windows: vec![],
            current_window: None,
        })
    }
}

/// split state by time
#[derive(Debug)]
pub struct TumblingWindow<V, S>
where
    V: Value + Debug,
    V::KeyValue: Debug,
    V::Selector: Clone + Debug,
    S: WindowStates<V>,
{
    config: WindowConfig<V>,
    current_window: Option<TimeWindow<V, S>>,
    _future_windows: Vec<TimeWindow<V, S>>,
}

impl<V, S> TumblingWindow<V, S>
where
    V: Value + Debug + Clone,
    V::KeyValue: Debug,
    V::Selector: Clone + Debug,
    S: WindowStates<V>,
    V::KeyValue: PartialEq + Eq + Hash + Clone,
{
    pub fn builder() -> WindowConfigBuilder<V> {
        WindowConfigBuilder::default()
    }

    pub fn current_window(&self) -> Option<&TimeWindow<V, S>> {
        self.current_window.as_ref()
    }

    /// add new value based on time
    /// if time is not found, it will be created
    /// if current window is expired, previous will be returned
    /// otherwise will return None
    pub fn add(&mut self, value: V) -> Result<Option<TimeWindow<V, S>>> {
        if let Some(event_time) = value.time() {
            let window_base = event_time.align_seconds(self.config.window_size_sec as u32);

            let key = match value.key(&self.config.key_selector)? {
                Some(key) => key,
                None => return Ok(None),
            };

            if let Some(current_window) = &mut self.current_window {
                if let Some(value_value) = value.value(&self.config.value_selector)? {
                    // current window exists
                    if let Some(new_value) =
                        current_window.add(&event_time, key.clone(), value_value)
                    {
                        // current window is full, we need to create new window
                        let mut current_window =
                            TimeWindow::new(window_base, self.config.window_size_sec);
                        current_window.add(&event_time, key, new_value);
                        Ok(std::mem::replace(
                            &mut self.current_window,
                            Some(current_window),
                        ))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            } else {
                let mut current_window = TimeWindow::new(window_base, self.config.window_size_sec);
                if let Some(value_value) = value.value(&self.config.value_selector)? {
                    current_window.add(&event_time, key, value_value);
                    self.current_window = Some(current_window);
                    Ok(None)
                } else {
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct NoKeySelector();

impl Selector for NoKeySelector {}

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
    use anyhow::Result;

    use crate::mean::RollingMean;
    use crate::time::FluvioTime;
    use crate::window::MICRO_PER_SEC;

    use super::{TumblingWindow, TimeWindow, NoKeySelector};
    use super::{Value, WindowStates};

    type KEY = u16;

    const VEH1: KEY = 22;
    const VEH2: KEY = 33;

    #[derive(Debug, Default, Clone, PartialEq)]
    struct TestValue {
        speed: f64,
        vehicle: KEY,
        time: DateTime<Utc>,
    }

    impl Value for TestValue {
        type KeyValue = KEY;
        type Selector = NoKeySelector;
        type Value = f64;

        fn key(&self, _selector: &NoKeySelector) -> Result<Option<Self::KeyValue>> {
            Ok(Some(self.vehicle))
        }

        fn time(&self) -> Option<FluvioTime> {
            Some(self.time).map(|time| time.into())
        }

        fn value(&self, _selector: &Self::Selector) -> Result<Option<Self::Value>> {
            Ok(Some(self.speed))
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

        fn add(&mut self, _key: KEY, value: f64) {
            self.speed.add(value);
        }
    }

    type DefaulTumblingWindow = TumblingWindow<TestValue, TestState>;

    type DefaultTimeWindow = TimeWindow<TestValue, TestState>;
    #[test]
    fn test_window_add() {
        let mut w = DefaultTimeWindow::new(
            FluvioTime::parse_from_str("2023-06-22T19:45:20.000Z").unwrap(),
            10,
        );

        let v1 = TestValue {
            speed: 3.2,
            vehicle: VEH1,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.132Z", "%+")
                .expect("parse")
                .into(),
        };

        assert!(w.add(&v1.time().unwrap(), VEH1, v1.speed).is_none());

        let v2 = TestValue {
            speed: 3.2,
            vehicle: VEH1,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:50.132Z", "%+")
                .expect("parse")
                .into(),
        };

        let out = w.add(&v2.time().unwrap(), VEH1, v2.speed);
        assert!(out.is_some());
        assert_eq!(out.unwrap(), v2.speed);
    }

    #[test]
    fn test_add_to_states() {
        let mut window: TumblingWindow<TestValue, TestState> = DefaulTumblingWindow::builder()
            .window_size_sec(10 as u16)
            .key_selector(NoKeySelector::default())
            .value_selector(NoKeySelector::default())
            .build()
            .expect("config failure");

        assert!(window.current_window.is_none());

        let v1 = TestValue {
            speed: 3.2,
            vehicle: VEH1,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.132Z", "%+")
                .expect("parse")
                .into(),
        };

        assert!(window.add(v1).expect("add").is_none());
        assert!(window.current_window.is_some());
        let current_window = window.current_window.as_ref().unwrap();
        assert_eq!(
            current_window.start,
            FluvioTime::parse_from_str("2023-06-22T19:45:20.000Z").unwrap()
        );
        assert_eq!(current_window.duration_in_micros(), 10 * MICRO_PER_SEC);
        assert_eq!(current_window.state.len(), 1);
        assert_eq!(current_window.state.get(&VEH1).unwrap().speed.mean(), 3.2);

        let v2 = TestValue {
            speed: 4.2,
            vehicle: VEH1,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:22.132Z", "%+")
                .expect("parse")
                .into(),
        };

        assert!(window.add(v2).expect("result").is_none());

        // try to add out of window

        let v3 = TestValue {
            speed: 4.2,
            vehicle: VEH1,
            time: DateTime::<FixedOffset>::parse_from_str("2023-06-22T19:45:35.132Z", "%+")
                .expect("parse")
                .into(),
        };

        assert!(window.add(v3).expect("result").is_some());
    }
}
