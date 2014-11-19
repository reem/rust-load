//! A work queue for scheduling units of work across threads in a fork-join fashion.
//!
//! Data associated with queues is simply a pair of unsigned integers. It is expected that a
//! higher-level API on top of this could allow safe fork-join parallelism.
//!
//! Extracted from Servo for general use.

// Timers
use std::io::timer::sleep;
use std::time::duration::Duration;

// Random number generation.
use rand::{Rng, XorShiftRng};
use std::rand::weak_rng;

use std::mem;
use std::sync::atomic::{AtomicUint, SeqCst};
use std::sync::deque::{Abort, BufferPool, Data, Empty, Stealer, Worker};


/// A unit of work.
///
/// # Type parameters
///
/// - `QueueData`: global custom data for the entire work queue.
/// - `WorkData`: custom data specific to each unit of work.
pub struct WorkUnit<QueueData, WorkData> {
    /// The function to execute.
    pub fun: fn(WorkData, &mut WorkerProxy<QueueData, WorkData>),
    /// Arbitrary data.
    pub data: WorkData,
}

/// Messages from the supervisor to the worker.
enum WorkerMsg<QueueData, WorkData> {
    /// Tells the worker to start work.
    Start(Worker<WorkUnit<QueueData, WorkData>>, *mut AtomicUint, *const QueueData),
    /// Tells the worker to stop. It can be restarted again with a `Start`.
    Stop,
    /// Tells the worker thread to terminate.
    Exit,
}

/// Messages to the supervisor
enum SupervisorMsg<QueueData, WorkData> {
    Finished,
    ReturnDeque(uint, Worker<WorkUnit<QueueData, WorkData>>)
}

/// Information that the supervisor thread keeps about the worker threads.
struct WorkerInfo<QueueData, WorkData> {
    /// The communication channel to the workers.
    chan: Sender<WorkerMsg<QueueData, WorkData>>,
    /// The worker end of the deque, if we have it.
    deque: Option<Worker<WorkUnit<QueueData, WorkData>>>,
    /// The thief end of the work-stealing deque.
    thief: Stealer<WorkUnit<QueueData, WorkData>>,
}

/// Information specific to each worker thread that the thread keeps.
struct WorkerThread<QueueData, WorkData> {
    /// The index of this worker.
    index: uint,
    /// The communication port from the supervisor.
    port: Receiver<WorkerMsg<QueueData, WorkData>>,
    /// The communication channel on which messages are sent to the supervisor.
    chan: Sender<SupervisorMsg<QueueData, WorkData>>,
    /// The thief end of the work-stealing deque for all other workers.
    other_deques: Vec<Stealer<WorkUnit<QueueData, WorkData>>>,
    /// The random number generator for this worker.
    rng: XorShiftRng,
}

const SPIN_COUNT: u32 = 128;
const SPINS_UNTIL_BACKOFF: u32 = 100;
const BACKOFF_INCREMENT_IN_US: u32 = 5;

impl<QueueData: Send, WorkData: Send> WorkerThread<QueueData, WorkData> {
    /// The main logic. This function starts up the worker and listens for
    /// messages.
    fn start(&mut self) {
        loop {
            // Wait for a start message.
            let (mut deque, ref_count, queue_data) = match self.port.recv() {
                WorkerMsg::Start(deque, ref_count, queue_data) => (deque, ref_count, queue_data),
                WorkerMsg::Stop => panic!("unexpected stop message"),
                WorkerMsg::Exit => return,
            };

            let mut back_off_sleep = 0 as u32;

            // We're off!
            //
            // FIXME(pcwalton): Can't use labeled break or continue cross-crate due to a Rust bug.
            loop {
                // FIXME(pcwalton): Nasty workaround for the lack of labeled break/continue
                // cross-crate.
                let mut work_unit = unsafe {
                    mem::uninitialized()
                };
                match deque.pop() {
                    Some(work) => work_unit = work,
                    None => {
                        // Become a thief.
                        let mut i = 0;
                        let mut should_continue = true;
                        loop {
                            let victim = (self.rng.next_u32() as uint) % self.other_deques.len();
                            match self.other_deques[victim].steal() {
                                Empty | Abort => {
                                    // Continue.
                                }
                                Data(work) => {
                                    work_unit = work;
                                    back_off_sleep = 0 as u32;
                                    break
                                }
                            }

                            if i > SPINS_UNTIL_BACKOFF {
                                sleep(Duration::microseconds(back_off_sleep as i64));
                                back_off_sleep += BACKOFF_INCREMENT_IN_US;
                            }

                            if i == SPIN_COUNT {
                                match self.port.try_recv() {
                                    Ok(WorkerMsg::Stop) => {
                                        should_continue = false;
                                        break
                                    }
                                    Ok(WorkerMsg::Exit) => return,
                                    Ok(_) => panic!("unexpected message"),
                                    _ => {}
                                }

                                i = 0
                            } else {
                                i += 1
                            }
                        }

                        if !should_continue {
                            break
                        }
                    }
                }

                // At this point, we have some work. Perform it.
                let mut proxy = WorkerProxy {
                    worker: &mut deque,
                    ref_count: ref_count,
                    queue_data: queue_data,
                };
                (work_unit.fun)(work_unit.data, &mut proxy);

                // The work is done. Now decrement the count of outstanding work items. If this was
                // the last work unit in the queue, then send a message on the channel.
                unsafe {
                    if (*ref_count).fetch_sub(1, SeqCst) == 1 {
                        self.chan.send(SupervisorMsg::Finished)
                    }
                }
            }

            // Give the deque back to the supervisor.
            self.chan.send(SupervisorMsg::ReturnDeque(self.index, deque))
        }
    }
}

/// A handle to the work queue that individual work units have.
pub struct WorkerProxy<'a, QueueData: 'a, WorkData: 'a> {
    worker: &'a mut Worker<WorkUnit<QueueData, WorkData>>,
    ref_count: *mut AtomicUint,
    queue_data: *const QueueData,
}

impl<'a, QueueData: 'static, WorkData: Send> WorkerProxy<'a, QueueData, WorkData> {
    /// Enqueues a block into the work queue.
    #[inline]
    pub fn push(&mut self, work_unit: WorkUnit<QueueData, WorkData>) {
        unsafe { drop((*self.ref_count).fetch_add(1, SeqCst)); }
        self.worker.push(work_unit);
    }

    /// Retrieves the queue user data.
    #[inline]
    pub fn user_data<'a>(&'a self) -> &'a QueueData {
        unsafe { mem::transmute(self.queue_data) }
    }
}

/// A work queue on which units of work can be submitted.
pub struct WorkQueue<QueueData, WorkData> {
    /// Information about each of the workers.
    workers: Vec<WorkerInfo<QueueData, WorkData>>,
    /// A port on which deques can be received from the workers.
    port: Receiver<SupervisorMsg<QueueData, WorkData>>,
    /// The amount of work that has been enqueued.
    work_count: uint,
    /// Arbitrary user data.
    pub data: QueueData,
}

impl<QueueData: Send, WorkData: Send> WorkQueue<QueueData, WorkData> {
    /// Creates a new work queue and spawns all the threads associated with
    /// it.
    pub fn new(thread_count: uint,
               user_data: QueueData) -> WorkQueue<QueueData, WorkData> {
        // Set up data structures.
        let (supervisor_chan, supervisor_port) = channel();
        let (mut infos, mut threads) = (vec!(), vec!());
        for i in range(0, thread_count) {
            let (worker_chan, worker_port) = channel();
            let pool = BufferPool::new();
            let (worker, thief) = pool.deque();
            infos.push(WorkerInfo {
                chan: worker_chan,
                deque: Some(worker),
                thief: thief,
            });
            threads.push(WorkerThread {
                index: i,
                port: worker_port,
                chan: supervisor_chan.clone(),
                other_deques: vec!(),
                rng: weak_rng(),
            });
        }

        // Connect workers to one another.
        for i in range(0, thread_count) {
            for j in range(0, thread_count) {
                if i != j {
                    threads[i].other_deques.push(infos[j].thief.clone())
                }
            }
        }

        // Spawn threads.
        for thread in threads.into_iter() {
            spawn(proc() {
                let mut thread = thread;
                thread.start()
            })
        }

        WorkQueue {
            workers: infos,
            port: supervisor_port,
            work_count: 0,
            data: user_data,
        }
    }

    /// Enqueues a block into the work queue.
    #[inline]
    pub fn push(&mut self, work_unit: WorkUnit<QueueData, WorkData>) {
        match &mut self.workers[0].deque {
            &None => {
                panic!("tried to push a block but we don't have the deque?!")
            }
            &Some(ref mut deque) => deque.push(work_unit),
        }
        self.work_count += 1
    }

    /// Synchronously runs all the enqueued tasks and waits for them to complete.
    pub fn run(&mut self) {
        // Tell the workers to start.
        let mut work_count = AtomicUint::new(self.work_count);
        for worker in self.workers.iter_mut() {
            worker.chan.send(WorkerMsg::Start(worker.deque.take().unwrap(),
                                              &mut work_count, &self.data))
        }

        // Wait for the work to finish.
        drop(self.port.recv());
        self.work_count = 0;

        // Tell everyone to stop.
        for worker in self.workers.iter() {
            worker.chan.send(WorkerMsg::Stop)
        }

        // Get our deques back.
        for _ in range(0, self.workers.len()) {
            match self.port.recv() {
                SupervisorMsg::ReturnDeque(index, deque) => self.workers[index].deque = Some(deque),
                SupervisorMsg::Finished => panic!("unexpected finished message!"),
            }
        }
    }

    /// Shutdown the workqueue.
    pub fn shutdown(&mut self) {
        for worker in self.workers.iter() {
            worker.chan.send(WorkerMsg::Exit)
        }
    }
}

