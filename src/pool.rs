//! A load-balancing task pool, extracted from Cargo and modified slightly
//! for regular use.
//!
//! Unlike std::sync::TaskPool, work registered on this pool can be
//! used by any of the waiting tasks.

use std::sync::{Arc, Mutex};

/// A load-balancing task pool.
pub struct TaskPool {
    tx: Sender<proc(): Send>
}

impl TaskPool {
    /// Create a new TaskPool
    ///
    /// ## Panic
    ///
    /// Panics if the number of tasks is < 0.
    pub fn new(tasks: uint) -> TaskPool {
        assert!(tasks > 0);

        let (tx, rx) = channel::<proc(): Send>();

        // Initialize the task pool in another thread.
        spawn(proc() {
            let state = Arc::new(Mutex::new(rx));

            for _ in range(0, tasks) {
                let rx = state.clone();
                spawn(proc() {
                    loop {
                        let job = rx.lock().recv_opt();
                        match job {
                            Ok(job) => job(),
                            Err(..) => break
                        }
                    }
                });
            }
        });

        TaskPool { tx: tx }
    }

    /// Run this proc in any of the tasks in the pool.
    pub fn execute(&self, job: proc(): Send) {
        self.tx.send(job);
    }
}

