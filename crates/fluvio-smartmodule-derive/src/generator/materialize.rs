use quote::quote;
use proc_macro2::TokenStream;

use crate::SmartModuleFn;
use crate::util::ident;

use super::transform::generate_transform;

pub fn generate_materialize_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_fn = &func.name;
    let user_code = func.func;

    let function_call = quote!(
        super:: #user_fn(&record, &mut state)
    );

    generate_transform(
        ident("filter_map"),
        user_code,
        quote! {
            // bring state from memory
            use fluvio_smartmodule::state::SmartModuleStateManager;

            let mut state_manager = if let Ok(state) = SmartModuleStateManager::init(Default::default()) {
                state
            } else {
                // here we should change the error type
                return SmartModuleTransformErrorStatus::DecodingBaseInput as i32;
            };

            let mut state = state_manager.restore();

            for mut record in records.into_iter() {
                let result = #function_call;
                match result {
                    Ok(Some((maybe_key, value))) => {
                        record.key = maybe_key;
                        record.value = value;
                        output.successes.push(record);
                    }
                    Ok(None) => {},
                Err(err) => {
                        let error = SmartModuleTransformRuntimeError::new(
                            &record,
                            base_offset,
                            SmartModuleKind::Filter,
                            err,
                        );
                        output.error = Some(error);
                        break;
                    }
                }
            }
            state_manager.save();
        },
    )
}
