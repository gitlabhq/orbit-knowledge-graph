mod lang;
mod registry;

pub use lang::Language;
pub use registry::{
    detect_language_from_extension, detect_language_from_name, detect_language_from_path,
    get_supported_extensions, ALL_LANGUAGES,
};
