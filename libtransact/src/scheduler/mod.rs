/*
 * Copyright 2019 Bitwise IO, Inc.
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

//! Batch scheduling with transaction execution APIs
//!
//! A `Scheduler` is used to execute one or more `Batch` objects, receiving
//! a `BatchExecutionResult` as a result of the execution of a `Batch`.  A `Batch` contains one or
//! more `Transaction` objects; each `Transaction` within a `Batch` is executed and then
//! consolidated to generate a `BatchExecutionResult`.
//!
//! In order for a `Scheduler` to execute batches, its associated `SchedulerExecutionInterface`
//! must be consumed by a component responsible for iterating over the `Transaction`s and providing
//! `TransactionExecutionResult`s back to the `Scheduler` via the `SchedulerExecutionInterface`.

pub mod multi;
pub mod parallel;
pub mod serial;

use crate::context::ContextId;
use crate::protocol::batch::BatchPair;
use crate::protocol::receipt::TransactionReceipt;
use crate::protocol::transaction::TransactionPair;

/// A transation and associated information required to execute it.
pub struct ExecutionTask {
    pair: TransactionPair,
    context_id: ContextId,
}

impl ExecutionTask {
    /// Create a new `ExecutionPair`.
    pub fn new(pair: TransactionPair, context_id: ContextId) -> Self {
        ExecutionTask { pair, context_id }
    }

    /// The transaction to be executed.
    pub fn pair(&self) -> &TransactionPair {
        &self.pair
    }

    /// The identifier of the context to be used when accessing state.
    pub fn context_id(&self) -> &ContextId {
        &self.context_id
    }

    /// Decompose into its components.
    pub fn take(self) -> (TransactionPair, ContextId) {
        (self.pair, self.context_id)
    }
}

/// Result from executing an invalid transaction.
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct InvalidTransactionResult {
    /// Transaction identifier.
    pub transaction_id: String,

    /// Human-readable reason explaining why the transaction was invalid.
    pub error_message: String,

    /// Transaction-specific error data which can be interpreted by clients
    /// familiar with this transaction's family.
    pub error_data: Vec<u8>,
}

/// Result from executing a transaction.
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum TransactionExecutionResult {
    /// The transation was invalid.
    Invalid(InvalidTransactionResult),

    /// The transation was valid and execution produced a TransactionReceipt.
    Valid(TransactionReceipt),
}

/// Result of executing a batch.
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct BatchExecutionResult {
    /// The `BatchPair` which was executed.
    pub batch: BatchPair,

    /// The results for each transaction in the batch.
    pub results: Vec<TransactionExecutionResult>,
}

#[derive(Debug, PartialEq)]
pub enum ExecutionTaskCompletionNotification {
    /// The transation was invalid.
    Invalid(ContextId, InvalidTransactionResult),

    /// The transation was valid (String is transaction ID).
    Valid(ContextId, String),
}

#[derive(Clone, Debug)]
pub enum SchedulerError {
    /// The scheduler's `add_batch` method was called with a batch that the scheduler already has
    /// pending or in progress; the contained `String` is the batch ID.
    DuplicateBatch(String),
    /// An internal error occurred that the scheduler could not recover from.
    Internal(String),
    /// A scheduler only has one task iterator, so its `take_task_iterator` method can only be
    /// called once.
    NoTaskIterator,
    /// The scheduler's `add_batch` method was called, but the scheduler was already finalized
    SchedulerFinalized,
    /// An `ExecutionTaskCompletionNotification` was received for a transaction that the scheduler
    /// was not expecting; the contained `String` is the transaction ID.
    UnexpectedNotification(String),
}

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            SchedulerError::DuplicateBatch(ref batch_id) => {
                write!(f, "duplicate batch added to scheduler: {}", batch_id)
            }
            SchedulerError::Internal(ref err) => {
                write!(f, "scheduler encountered an internal error: {}", err)
            }
            SchedulerError::NoTaskIterator => write!(f, "task iterator already taken"),
            SchedulerError::SchedulerFinalized => write!(f, "batch added to finalized scheduler"),
            SchedulerError::UnexpectedNotification(ref txn_id) => write!(
                f,
                "scheduler received an unexpected notification: {}",
                txn_id
            ),
        }
    }
}

/// Schedules batches and transactions and returns execution results.
pub trait Scheduler {
    /// Sets a callback to receive results from processing batches. The order
    /// the results are received is not guarenteed to be the same order as the
    /// batches were added with `add_batch`. If callback is called with None,
    /// all batch results have been sent (only used when the scheduler has been
    /// finalized and no more batches will be added).
    fn set_result_callback(
        &mut self,
        callback: Box<Fn(Option<BatchExecutionResult>) + Send>,
    ) -> Result<(), SchedulerError>;

    /// Sets a callback to receive any errors encountered by the Scheduler that are not related to
    /// a specific batch.
    fn set_error_callback(
        &mut self,
        callback: Box<Fn(SchedulerError) + Send>,
    ) -> Result<(), SchedulerError>;

    /// Adds a BatchPair to the scheduler.
    fn add_batch(&mut self, batch: BatchPair) -> Result<(), SchedulerError>;

    /// Drops any unscheduled transactions from this scheduler. Any already
    /// scheduled transactions will continue to execute.
    ///
    /// Returns a `Vec` of the dropped `BatchPair`s.
    fn cancel(&mut self) -> Result<Vec<BatchPair>, SchedulerError>;

    /// Finalizes the scheduler, which will disable the ability to add more
    /// batches. After this is called, `add_batch()` will be return a
    /// FinalizedSchedulerError.
    fn finalize(&mut self) -> Result<(), SchedulerError>;

    /// Returns an iterator that returns transactions to be executed.
    fn take_task_iterator(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = ExecutionTask> + Send>, SchedulerError>;

    /// Returns a newly allocated ExecutionTaskCompletionNotifier which allows
    /// sending a notification to the scheduler that indicates the task has
    /// been executed.
    fn new_notifier(&mut self) -> Result<Box<dyn ExecutionTaskCompletionNotifier>, SchedulerError>;
}

/// Allows sending a notification to the scheduler that execution of a task
/// has completed.
pub trait ExecutionTaskCompletionNotifier: Send {
    /// Sends a notification to the scheduler.
    fn notify(&self, notification: ExecutionTaskCompletionNotification);

    fn clone_box(&self) -> Box<dyn ExecutionTaskCompletionNotifier>;
}

impl Clone for Box<ExecutionTaskCompletionNotifier> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

fn default_result_callback(batch_result: Option<BatchExecutionResult>) {
    warn!(
        "No result callback set; dropping batch execution result: {}",
        match batch_result {
            Some(ref result) => result.batch.batch().header_signature(),
            None => "None",
        }
    );
}

fn default_error_callback(error: SchedulerError) {
    error!("No error callback set; SchedulerError: {}", error);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::manager::ContextManagerError;
    use crate::context::ContextLifecycle;
    use crate::workload::xo::XoBatchWorkload;
    use crate::workload::BatchWorkload;

    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;

    pub fn valid_result_from_batch(batch: BatchPair) -> Option<BatchExecutionResult> {
        let results = batch
            .batch()
            .transactions()
            .iter()
            .map(|txn| {
                TransactionExecutionResult::Valid(TransactionReceipt {
                    state_changes: vec![],
                    events: vec![],
                    data: vec![],
                    transaction_id: txn.header_signature().into(),
                })
            })
            .collect();
        Some(BatchExecutionResult { batch, results })
    }

    pub fn invalid_result_from_batch(batch: BatchPair) -> Option<BatchExecutionResult> {
        let results = batch
            .batch()
            .transactions()
            .iter()
            .map(|txn| {
                TransactionExecutionResult::Invalid(InvalidTransactionResult {
                    transaction_id: txn.header_signature().into(),
                    error_message: String::new(),
                    error_data: vec![],
                })
            })
            .collect();
        Some(BatchExecutionResult { batch, results })
    }

    pub fn mock_context_id() -> ContextId {
        [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ]
    }

    #[derive(Clone)]
    pub struct MockContextLifecycle {}

    impl MockContextLifecycle {
        pub fn new() -> Self {
            MockContextLifecycle {}
        }
    }

    impl ContextLifecycle for MockContextLifecycle {
        fn create_context(
            &mut self,
            _dependent_contexts: &[ContextId],
            _state_id: &str,
        ) -> ContextId {
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x01,
            ]
        }

        fn get_transaction_receipt(
            &self,
            _context_id: &ContextId,
            _transaction_id: &str,
        ) -> Result<TransactionReceipt, ContextManagerError> {
            unimplemented!()
        }

        fn drop_context(&mut self, _context_id: ContextId) {}
    }

    /// Attempt to add a batch to the scheduler; attempt to add the batch again and verify that a
    /// `DuplicateBatch` error is returned. Return the batch so the calling test can verify other
    /// expected behavior.
    pub fn test_scheduler_add_batch(scheduler: &mut Scheduler) -> BatchPair {
        let batch = XoBatchWorkload::new_with_seed(0)
            .next_batch()
            .expect("Failed to get batch");
        scheduler
            .add_batch(batch.clone())
            .expect("Failed to add batch");
        match scheduler.add_batch(batch.clone()) {
            Err(SchedulerError::DuplicateBatch(batch_id)) => {
                assert_eq!(batch_id, batch.batch().header_signature())
            }
            res => panic!("Did not get DuplicateBatch; got {:?}", res),
        }
        batch
    }

    /// Add two batches to the scheduler, then attempt to cancel it twice; verify that it properly
    /// drains and returns the pending batches.
    pub fn test_scheduler_cancel(scheduler: &mut Scheduler) {
        let mut workload = XoBatchWorkload::new_with_seed(1);
        let batches = (0..2)
            .map(|_| workload.next_batch().expect("Failed to get batch"))
            .collect::<Vec<_>>();

        scheduler
            .add_batch(batches[0].clone())
            .expect("Failed to add 1st batch");
        scheduler
            .add_batch(batches[1].clone())
            .expect("Failed to add 2nd batch");

        for batch in scheduler.cancel().expect("Failed 1st cancel") {
            assert!(batches.contains(&batch));
        }
        assert!(scheduler.cancel().expect("Failed 2nd cancel").is_empty());
    }

    /// Tests a simple scheduler worklfow of processing a single transaction.
    ///
    /// For the purposes of this test, we simply return an invalid transaction
    /// as we are not testing the actual execution of the transaction but
    /// rather the flow of getting a result after adding the batch.
    pub fn test_scheduler_flow_with_one_transaction(scheduler: &mut Scheduler) {
        let shared = Arc::new((Mutex::new(false), Condvar::new()));
        let shared2 = shared.clone();

        let mut workload = XoBatchWorkload::new_with_seed(8);

        scheduler
            .set_result_callback(Box::new(move |_batch_result| {
                let &(ref lock, ref cvar) = &*shared2;

                let mut inner = lock.lock().unwrap();
                *inner = true;
                cvar.notify_one();
            }))
            .expect("Failed to set result callback");

        let mut task_iterator = scheduler
            .take_task_iterator()
            .expect("Failed to get task iterator");
        let notifier = scheduler
            .new_notifier()
            .expect("Failed to get new notifier");

        thread::Builder::new()
            .name(String::from(
                "Thread-test_scheduler_flow_with_one_transaction",
            ))
            .spawn(move || loop {
                let context_id = [
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x01,
                ];
                match task_iterator.next() {
                    Some(task) => {
                        notifier.notify(ExecutionTaskCompletionNotification::Invalid(
                            context_id,
                            InvalidTransactionResult {
                                transaction_id: task
                                    .pair()
                                    .transaction()
                                    .header_signature()
                                    .to_string(),
                                error_message: String::from("invalid"),
                                error_data: vec![],
                            },
                        ));
                    }
                    None => {
                        break;
                    }
                }
            })
            .unwrap();

        scheduler
            .add_batch(workload.next_batch().unwrap())
            .expect("Failed to add batch");

        let &(ref lock, ref cvar) = &*shared;

        let mut inner = lock.lock().unwrap();
        while !(*inner) {
            inner = cvar.wait(inner).unwrap();
        }
    }
}
