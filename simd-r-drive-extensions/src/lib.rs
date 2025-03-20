#[cfg(doctest)]
doc_comment::doctest!("../README.md");

pub mod utils;
pub use utils::option_serializer::*;

mod storage_option_ext;
pub use storage_option_ext::*;

mod storage_cache_ext;
pub use storage_cache_ext::*;

mod constants;

pub mod namespace_hasher;
pub use namespace_hasher::NamespaceHasher;
