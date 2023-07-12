use std::sync::{Mutex, OnceLock};

use fluvio_protocol::{Encoder, Decoder};

use crate::input::SmartModuleExtraParams;

static INNER_STATE: OnceLock<Mutex<Vec<u8>>> = OnceLock::new();

pub trait SmartModuleState: Default + Encoder + Decoder {
    fn init(_params: SmartModuleExtraParams) -> Self {
        Self::default()
    }

    fn update_state(&mut self, other: Self) {
        *self = other;
    }
}

pub struct SmartModuleStateManager<T: SmartModuleState> {
    state: T,
}

impl<T: SmartModuleState> SmartModuleStateManager<T> {
    pub fn init(params: SmartModuleExtraParams) -> Result<Self, super::Error> {
        let state = INNER_STATE.get_or_init(|| Mutex::new(Vec::new()));
        let state: &Vec<u8> = &*state.lock().unwrap();

        let state = if state.is_empty() {
            T::init(params)
        } else {
            T::decode_from(&mut std::io::Cursor::new(state), 0)?
        };
        Ok(Self { state })
    }
    pub fn restore(&mut self) -> &mut T {
        &mut self.state
    }

    pub fn save(&self) {
        let state = INNER_STATE.get().unwrap();
        let mut out = vec![];
        self.state.encode(&mut out, 0).unwrap();
        *state.lock().unwrap() = out;
    }
}
