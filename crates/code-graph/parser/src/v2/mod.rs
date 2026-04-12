mod convert;
pub mod csharp;
pub mod java;
pub mod kotlin;
mod languages;
pub mod python;

pub use convert::CanonicalParser;
pub use languages::{
    detect_language_from_extension, detect_language_from_path, get_supported_extensions, parse_file,
};
