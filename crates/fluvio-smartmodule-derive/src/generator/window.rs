use quote::quote;
use proc_macro2::TokenStream;
use crate::SmartModuleFn;

// generate window
pub fn generate_window_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_fn = func.name;
    let user_code = func.func;
    quote! {

        #[allow(dead_code)]
        #user_code

        #[cfg(target_arch = "wasm32")]
        mod ___system {

            #[no_mangle]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn window(ptr: *mut u8, len: usize, version: i16) -> i32 {
                use fluvio_smartmodule::dataplane::smartmodule::{
                    SmartModuleWindowInput, SmartModuleKind, window::SmartModuleWindowErrorStatus,
                    SmartModuleWindowOutput, SmartModuleWindowRuntimeError,
                };
                use fluvio_smartmodule::dataplane::core::{Encoder, Decoder};
                use fluvio_smartmodule::dataplane::record::{Record, RecordKey, RecordData};

                extern "C" {
                    fn copy_records(putr: i32, len: i32);
                }

                let input_data = Vec::from_raw_parts(ptr, len, len);
                let mut smartmodule_input = SmartModuleWindowInput::default();
                // TODO: Change to WindowErrorStatus
                if let Err(_err) = Decoder::decode(&mut smartmodule_input, &mut std::io::Cursor::new(input_data), version) {
                    return SmartModuleWindowErrorStatus::DecodingInput as i32;
                }


                let result = super:: #user_fn(smartmodule_input);
                let mut output = SmartModuleWindowOutput {
                     successes: vec![],
                     error: None,
                };


                match result {
                    Ok(output_records) => {
                        for (output_key, output_value) in output_records {
                            let key = RecordKey::from_option(output_key);
                            let new_record = Record::new_key_value(key, output_value);
                            output.successes.push(new_record);
                        }
                    }

                    Err(err) => {
                        let error = SmartModuleWindowRuntimeError::new(
                            err,
                        );
                        output.error = Some(error);
                    }
                }
                // ENCODING
                let mut out = vec![];
                if let Err(_) = Encoder::encode(&mut output, &mut out, version) {
                    return SmartModuleWindowErrorStatus::EncodingOutput as i32;
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
