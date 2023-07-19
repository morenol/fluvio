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

#[derive(Debug, Default, Clone)]
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

#[derive(Debug, Default)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize))]
pub struct RollingSum<A> {
    #[cfg_attr(feature = "use_serde", serde(skip))]
    count: u32,
    sum: A,
}

impl<A> RollingSum<A>
where
    A: Default + Copy + std::ops::Add<Output = A> + std::ops::AddAssign,
{
    /// add to sample
    pub fn add(&mut self, value: A) {
        self.sum = self.sum + value;
        self.count = self.count + 1;
    }

    pub fn sum(&self) -> A {
        self.sum
    }
}

#[cfg(test)]
mod mean_test {

    use super::*;

    #[test]
    fn rolling_mean() {
        let mut rm = RollingMean::default();
        rm.add(3.2);
        assert_eq!(rm.mean(), 3.2);
        rm.add(4.2);
        assert_eq!(rm.mean(), 3.7);
    }

    #[test]
    fn rolling_sum() {
        let mut rm = RollingSum::default();
        rm.add(4);
        assert_eq!(rm.sum(), 4);
        rm.add(6);
        assert_eq!(rm.sum(), 10);
    }
}
