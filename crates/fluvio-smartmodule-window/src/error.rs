use fluvio_protocol::{Encoder, Decoder};

#[repr(i32)]
#[derive(thiserror::Error, Debug, Clone, Eq, PartialEq, Encoder, Default, Decoder)]
#[non_exhaustive]
#[fluvio(encode_discriminant)]
pub enum SmartModuleWindowErrorStatus {
    #[error("encountered unknown error during SmartModule processing")]
    #[default]
    UnknownError = -1,
    #[error("failed to decode SmartModule init input")]
    DecodingInput = -10,
    #[error("failed to encode SmartModule init output")]
    EncodingOutput = -11,
}
