//! Helpers shared by the API examples.
//!
//! Cargo builds every file in `examples/` as its own binary, so one example
//! cannot import another. Including this module with
//! `#[path = "example_utils/mod.rs"] mod example_utils;` compiles it into each
//! example that needs it.
//!
//! The sources live in `crates/example_utils/`, outside both packages, so the
//! uploader examples use the same files: see that directory's README.
//!
//! Helpers that only one example calls are marked `#[allow(dead_code)]`: the
//! other examples compile them too, they just never use them.

#[path = "../../../example_utils/env.rs"]
pub mod env;

#[path = "../../../example_utils/login.rs"]
pub mod login;
