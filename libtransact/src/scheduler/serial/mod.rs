/*
 * Copyright 2019 Cargill Incorporated
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -----------------------------------------------------------------------------
 */

//! A `Scheduler` which schedules transaction for execution one at time.

mod core;
mod execution;
mod shared;

use crate::context::ContextLifecycle;
use crate::protocol::batch::BatchPair;
use crate::scheduler::BatchExecutionResult;
use crate::scheduler::ExecutionTask;
use crate::scheduler::ExecutionTaskCompletionNotifier;
use crate::scheduler::Scheduler;
use crate::scheduler::SchedulerError;

use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

// If the shared lock is poisoned, report an internal error since the scheduler cannot recover.
impl From<std::sync::PoisonError<std::sync::MutexGuard<'_, shared::Shared>>> for SchedulerError {
    fn from(
        error: std::sync::PoisonError<std::sync::MutexGuard<'_, shared::Shared>>,
    ) -> SchedulerError {
        SchedulerError::Internal(format!("scheduler shared lock is poisoned: {}", error))
    }
}

// If the core `Receiver` disconnects, report an internal error since the scheduler can't operate
// without the core thread.
impl From<std::sync::mpsc::SendError<core::CoreMessage>> for SchedulerError {
    fn from(error: std::sync::mpsc::SendError<core::CoreMessage>) -> SchedulerError {
        SchedulerError::Internal(format!("scheduler's core thread disconnected: {}", error))
    }
}

/// A `Scheduler` implementation which schedules transactions for execution
/// one at a time.
pub struct SerialScheduler {
    shared_lock: Arc<Mutex<shared::Shared>>,
    core_handle: Option<std::thread::JoinHandle<()>>,
    core_tx: Sender<core::CoreMessage>,
    task_iterator: Option<Box<Iterator<Item = ExecutionTask> + Send>>,
}

impl SerialScheduler {
    /// Returns a newly created `SerialScheduler`.
    pub fn new(
        context_lifecycle: Box<ContextLifecycle>,
        state_id: String,
    ) -> Result<SerialScheduler, SchedulerError> {
        let (execution_tx, execution_rx) = mpsc::channel();
        let (core_tx, core_rx) = mpsc::channel();

        let shared_lock = Arc::new(Mutex::new(shared::Shared::new()));

        // Start the thread to accept and process CoreMessage messages
        let core_handle = core::SchedulerCore::new(
            shared_lock.clone(),
            core_rx,
            execution_tx,
            context_lifecycle,
            state_id,
        )
        .start()?;

        Ok(SerialScheduler {
            shared_lock,
            core_handle: Some(core_handle),
            core_tx: core_tx.clone(),
            task_iterator: Some(Box::new(execution::SerialExecutionTaskIterator::new(
                core_tx,
                execution_rx,
            ))),
        })
    }

    pub fn shutdown(mut self) {
        match self.core_tx.send(core::CoreMessage::Shutdown) {
            Ok(_) => {
                if let Some(join_handle) = self.core_handle.take() {
                    join_handle.join().unwrap_or_else(|err| {
                        // This should not never happen, because the core thread should never panic
                        error!(
                            "failed to join scheduler thread because it panicked: {:?}",
                            err
                        )
                    });
                }
            }
            Err(err) => {
                warn!("failed to send to scheduler thread during drop: {}", err);
            }
        }
    }
}

impl Scheduler for SerialScheduler {
    fn set_result_callback(
        &mut self,
        callback: Box<Fn(Option<BatchExecutionResult>) + Send>,
    ) -> Result<(), SchedulerError> {
        self.shared_lock.lock()?.set_result_callback(callback);
        Ok(())
    }

    fn set_error_callback(
        &mut self,
        callback: Box<Fn(SchedulerError) + Send>,
    ) -> Result<(), SchedulerError> {
        self.shared_lock.lock()?.set_error_callback(callback);
        Ok(())
    }

    fn add_batch(&mut self, batch: BatchPair) -> Result<(), SchedulerError> {
        let mut shared = self.shared_lock.lock()?;

        if shared.finalized() {
            return Err(SchedulerError::SchedulerFinalized);
        }

        if shared.batch_already_queued(&batch) {
            return Err(SchedulerError::DuplicateBatch(
                batch.batch().header_signature().into(),
            ));
        }

        shared.add_unscheduled_batch(batch);

        // Notify the core that a batch has been added. Note that the batch is
        // not sent across the channel because the batch has already been added
        // to the unscheduled queue above, where we hold a lock; adding a batch
        // must be exclusive with finalize.
        self.core_tx.send(core::CoreMessage::BatchAdded)?;

        Ok(())
    }

    fn cancel(&mut self) -> Result<Vec<BatchPair>, SchedulerError> {
        Ok(self.shared_lock.lock()?.drain_unscheduled_batches())
    }

    fn finalize(&mut self) -> Result<(), SchedulerError> {
        self.shared_lock.lock()?.set_finalized(true);
        self.core_tx.send(core::CoreMessage::Finalized)?;
        Ok(())
    }

    fn take_task_iterator(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = ExecutionTask> + Send>, SchedulerError> {
        self.task_iterator
            .take()
            .ok_or(SchedulerError::NoTaskIterator)
    }

    fn new_notifier(&mut self) -> Result<Box<dyn ExecutionTaskCompletionNotifier>, SchedulerError> {
        Ok(Box::new(
            execution::SerialExecutionTaskCompletionNotifier::new(self.core_tx.clone()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::tests::*;

    /// This test will hang if join() fails within the scheduler.
    #[test]
    fn test_scheduler_thread_cleanup() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        SerialScheduler::new(context_lifecycle, state_id)
            .expect("Failed to create scheduler")
            .shutdown();
    }

    /// In addition to the basic functionality verified by `test_scheduler_add_batch`, this test
    /// verifies that the SerialScheduler adds the batch to its unscheduled batches queue.
    #[test]
    fn test_serial_scheduler_add_batch() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");

        let batch = test_scheduler_add_batch(&mut scheduler);

        assert!(scheduler
            .shared_lock
            .lock()
            .expect("shared lock is poisoned")
            .batch_already_queued(&batch));

        scheduler.shutdown();
    }

    #[test]
    fn test_serial_scheduler_cancel() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");
        test_scheduler_cancel(&mut scheduler);
        scheduler.shutdown();
    }

    #[test]
    pub fn test_serial_scheduler_flow_with_one_transaction() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");
        test_scheduler_flow_with_one_transaction(&mut scheduler);
        scheduler.shutdown();
    }
}
