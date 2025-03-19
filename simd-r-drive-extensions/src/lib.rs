#[cfg(doctest)]
doc_comment::doctest!("../README.md");

mod storage_option_ext;
pub use storage_option_ext::*;

mod storage_cache_ext;
pub use storage_cache_ext::*;
