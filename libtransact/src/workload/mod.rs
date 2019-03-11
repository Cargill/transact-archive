/*
 * Copyright 2018 Bitwise IO, Inc.
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

pub mod command;
pub mod error;
pub mod xo;

use crate::protocol::batch::BatchPair;
use crate::protocol::transaction::TransactionPair;
use crate::workload::error::WorkloadError;

pub trait TransactionWorkload {
    fn next_transaction(&mut self) -> Result<TransactionPair, WorkloadError>;
}

pub trait BatchWorkload {
    fn next_batch(&mut self) -> Result<BatchPair, WorkloadError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn test_transaction_workload(workload: &mut TransactionWorkload) {
        workload.next_transaction().unwrap();
        workload.next_transaction().unwrap();
    }

    pub fn test_batch_workload(workload: &mut BatchWorkload) {
        workload.next_batch().unwrap();
        workload.next_batch().unwrap();
    }
}
