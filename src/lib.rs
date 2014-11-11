#![license = "Mozilla Publice License, v. 2.0"]
#![deny(missing_docs, warnings)]

//! A load balancing TaskPool and WorkQueue.
//!
//! Extracted from Cargo and Servo for general use.

extern crate rand;

pub use queue::WorkQueue;
pub mod queue;

