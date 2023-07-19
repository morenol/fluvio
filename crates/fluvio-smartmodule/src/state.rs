use std::{
    sync::{Mutex, OnceLock, MutexGuard},
    marker::PhantomData,
};

use crate::input::SmartModuleExtraParams;

pub trait SmartModuleState: Clone {
    fn init(params: SmartModuleExtraParams) -> super::Result<Self>;
}

pub trait StateManager<T: SmartModuleState> {
    fn init(&mut self, state: T) -> super::Result<()>;
    fn get_mut(&mut self) -> MutexGuard<'_, T>;
}

pub struct OnceLockManager<'a, T: SmartModuleState> {
    state: &'a OnceLock<Mutex<T>>,
}

impl<'a, T: SmartModuleState> From<&'a OnceLock<Mutex<T>>> for OnceLockManager<'a, T> {
    fn from(state: &'a OnceLock<Mutex<T>>) -> Self {
        Self { state }
    }
}

impl<'a, T: SmartModuleState> StateManager<T> for OnceLockManager<'a, T>
where
    T: std::fmt::Debug,
{
    fn init(&mut self, state: T) -> super::Result<()> {
        self.state
            .set(Mutex::new(state))
            .map_err(|_| crate::eyre!("state already initialized"))?;
        Ok(())
    }
    fn get_mut(&mut self) -> MutexGuard<'_, T> {
        // set was called so it should not panic
        let mutex = self.state.get().unwrap();
        mutex.lock().expect("failed to lock state")
    }
}

pub struct SmartModuleStateManager<
    'a,
    T: SmartModuleState,
    BS: StateManager<T> = OnceLockManager<'a, T>,
> {
    _phantom: PhantomData<&'a T>,
    backend: BS,
}

impl<'a, T: SmartModuleState, BS: StateManager<T>> SmartModuleStateManager<'a, T, BS> {
    pub fn create<P>(into_bs: P) -> Result<Self, super::Error>
    where
        P: Into<BS>,
    {
        let backend = into_bs.into();

        Ok(Self {
            _phantom: PhantomData::default(),
            backend,
        })
    }

    pub fn init<P>(into_bs: P, params: SmartModuleExtraParams) -> Result<(), super::Error>
    where
        P: Into<BS>,
    {
        let mut backend = into_bs.into();
        backend.init(T::init(params)?)?;
        Ok(())
    }
    pub fn restore(&mut self) -> MutexGuard<'_, T> {
        self.backend.get_mut()
    }
}

mod window {
    use std::fmt::Debug;
    use std::hash::Hash;

    use fluvio_smartmodule_window::window::{TumblingWindow, WindowStates, Value};

    use crate::input::SmartModuleExtraParams;

    use super::SmartModuleState;

    impl<V, S> SmartModuleState for TumblingWindow<V, S>
    where
        V: Value + Debug + Clone,
        V::KeyValue: Debug,
        V::Selector: Clone + Debug + Default,
        S: WindowStates<V> + Clone,
        V::KeyValue: PartialEq + Eq + Hash + Clone,
    {
        fn init(_params: SmartModuleExtraParams) -> super::super::Result<Self> {
            let key_selector: V::Selector = Default::default();
            let value_selector: V::Selector = Default::default();

            Ok(Self::builder()
                .window_size_sec(60 as u16)
                .key_selector(key_selector)
                .value_selector(value_selector)
                .build()
                .map_err(|err| super::super::eyre!("error initializing: {}", err))?)
        }
    }
}
