use quote::quote;
use syn::Type;
use proc_macro2::TokenStream;

use crate::SmartModuleFn;

pub fn generate_materialize_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_fn = &func.name;
    let user_code = func.func;

    // get state type, which is the second parameter of user_code
    let user_state_type =
        if let syn::FnArg::Typed(fn_sig_type) = func.func.sig.inputs.iter().nth(1).unwrap() {
            if let Type::Reference(ty) = *fn_sig_type.ty.clone() {
                ty.elem
            } else {
                fn_sig_type.ty.clone()
            }
        } else {
            panic!("unknown state type");
        };

    let function_call = quote!(
        super:: #user_fn(&record,  state)
    );

    quote! {


            #[allow(dead_code)]
            #user_code

            #[cfg(target_arch = "wasm32")]
            mod __system {

                static INNER_STATE: ::std::sync::OnceLock<::std::sync::Mutex<super::#user_state_type>> = ::std::sync::OnceLock::new();

                #[no_mangle]
                #[allow(clippy::missing_safety_doc)]
                pub unsafe fn init(ptr: *mut u8, len: usize, version: i16) -> i32 {
                    use fluvio_smartmodule::dataplane::smartmodule::{
                        SmartModuleInitError, SmartModuleInitInput,
                        SmartModuleInitOutput,
                        SmartModuleInitErrorStatus,
                        SmartModuleInitRuntimeError
                    };
                    use fluvio_smartmodule::dataplane::core::{Decoder,Encoder};

                    let input_data = Vec::from_raw_parts(ptr, len, len);
                    let mut input = SmartModuleInitInput::default();
                    if let Err(_err) =
                        Decoder::decode(&mut input, &mut std::io::Cursor::new(input_data), version)
                    {
                        return SmartModuleInitErrorStatus::DecodingInput as i32;
                    }

                    use ::fluvio_smartmodule::state::SmartModuleStateManager;

                    match SmartModuleStateManager::<super::#user_state_type, ::fluvio_smartmodule::state::OnceLockManager<_>>::init(&INNER_STATE, input.params) {
                        Ok(_) => 0,
                        Err(err) =>  {

                            // copy data from wasm memory
                            extern "C" {
                                fn copy_records(putr: i32, len: i32);
                            }

                            let mut output = SmartModuleInitOutput {
                                error: SmartModuleInitRuntimeError::new(err)
                            };

                            let mut out = vec![];
                            if let Err(_) = Encoder::encode(&output, &mut out, version) {
                                return SmartModuleInitErrorStatus::EncodingOutput as i32;
                            }

                            let out_len = out.len();
                            let ptr = out.as_mut_ptr();
                            std::mem::forget(out);
                            copy_records(ptr as i32, out_len as i32);

                            SmartModuleInitErrorStatus::InitError as i32
                        }
                    }
                }

                #[no_mangle]
                #[allow(clippy::missing_safety_doc)]
                pub unsafe fn filter_map(ptr: *mut u8, len: usize, version: i16) -> i32 {
                    use fluvio_smartmodule::dataplane::smartmodule::{
                        SmartModuleInput, SmartModuleTransformErrorStatus,
                        SmartModuleTransformRuntimeError, SmartModuleKind, SmartModuleOutput,
                    };
                    use fluvio_smartmodule::dataplane::core::{Encoder, Decoder};
                    use fluvio_smartmodule::dataplane::record::{Record, RecordData};

                    extern "C" {
                        fn copy_records(putr: i32, len: i32);
                    }

                    // DECODING

                    let input_data = Vec::from_raw_parts(ptr, len, len);
                    let mut smartmodule_input = SmartModuleInput::default();
                    if let Err(_err) = Decoder::decode(&mut smartmodule_input, &mut std::io::Cursor::new(input_data), version) {
                        return SmartModuleTransformErrorStatus::DecodingBaseInput as i32;
                    }

                    let base_offset = smartmodule_input.base_offset();
                    let records_input = smartmodule_input.into_raw_bytes();
                    let mut records: Vec<Record> = vec![];
                    if let Err(_err) = Decoder::decode(&mut records, &mut std::io::Cursor::new(records_input), version) {
                        return SmartModuleTransformErrorStatus::DecodingRecords as i32;
                    };

                    // PROCESSING
                    let mut output = SmartModuleOutput {
                        successes: Vec::with_capacity(records.len()),
                        error: None,
                    };

                    use ::std::ops::DerefMut;

                    use ::fluvio_smartmodule::state::SmartModuleStateManager;

                    let mut state_manager: SmartModuleStateManager<_> = if let Ok(state) = SmartModuleStateManager::create(&INNER_STATE) {
                        state
                    } else {
                        // here we should change the error type
                        return SmartModuleTransformErrorStatus::DecodingBaseInput as i32;
                    };

                    let mut state_guard = state_manager.restore();
                    let mut state = state_guard.deref_mut();

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

                    // ENCODING
                    let mut out = vec![];
                    if let Err(_) = Encoder::encode(&mut output, &mut out, version) {
                        return SmartModuleTransformErrorStatus::EncodingOutput as i32;
                    }

                    let out_len = out.len();
                    let ptr = out.as_mut_ptr();
                    std::mem::forget(out);
                    copy_records(ptr as i32, out_len as i32);
                    output.successes.len() as i32
                }
        }

    }
}
