use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::{Result, Ok};
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInitErrorStatus, SmartModuleWindowInput, SmartModuleWindowOutput,
};
use wasmtime::{AsContextMut, TypedFunc};

use super::instance::SmartModuleInstanceContext;

pub(crate) const WINDOW_FN_NAME: &str = "window";
type WasmInitFn = TypedFunc<(i32, i32, u32), i32>;

pub(crate) struct SmartModuleWindow(WasmInitFn);

impl Debug for SmartModuleWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WindowFn")
    }
}

impl SmartModuleWindow {
    /// Try to create filter by matching function, if function is not found, then return empty
    pub fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>> {
        match ctx.get_wasm_func(store, WINDOW_FN_NAME) {
            // check type signature
            Some(func) => func
                .typed(&mut *store)
                .or_else(|_| func.typed(store))
                .map(|init_fn| Some(Self(init_fn))),
            None => Ok(None),
        }
    }

    /// initialize SmartModule
    pub(crate) fn call(
        &mut self,
        input: &SmartModuleWindowInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<()> {
        let slice = ctx.write_input(input, &mut *store)?;
        let window_output = self.0.call(&mut *store, slice)?;

        if window_output < 0 {
            let internal_error = SmartModuleInitErrorStatus::try_from(window_output)
                .unwrap_or(SmartModuleInitErrorStatus::UnknownError);

            match internal_error {
                SmartModuleInitErrorStatus::InitError => {
                    let output: SmartModuleWindowOutput = ctx.read_output(store)?;
                    if let Some(error) = output.error {
                        Err(error.into())
                    } else {
                        Err(anyhow::anyhow!("Window Error not found"))
                    }
                }
                _ => Err(internal_error.into()),
            }
        } else {
            Ok(())
        }
    }
}
