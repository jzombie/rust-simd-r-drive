#[cfg(doctest)]
doc_comment::doctest!("../README.md");

mod storage_option_ext;
pub use storage_option_ext::*;
