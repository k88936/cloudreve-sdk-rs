//! Helpers shared by the uploader examples.
//!
//! Cargo builds every file in `examples/` as its own binary, so one example
//! cannot import another. Including this module with
//! `#[path = "example_utils/mod.rs"] mod example_utils;` compiles it into each
//! example that needs it.
//!
//! `env` and `login` come from `crates/example_utils/`, the copy the API
//! examples use too; only the helpers that need this crate live here.
//!
//! Helpers that only one example calls are marked `#[allow(dead_code)]`: the
//! other examples compile them too, they just never use them.

pub mod download;
pub mod drain;

#[path = "../../../example_utils/env.rs"]
pub mod env;

#[path = "../../../example_utils/login.rs"]
pub mod login;
