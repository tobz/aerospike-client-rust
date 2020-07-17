// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use std::cell::UnsafeCell;
use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::{iter, StreamExt};

use crate::batch::BatchRead;
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::BatchReadCommand;
use crate::errors::Result;
use crate::policy::{BatchPolicy, Concurrency};
use crate::Key;

pub struct BatchExecutor {
    cluster: Arc<Cluster>,
}

impl BatchExecutor {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        BatchExecutor {
            cluster,
        }
    }

    pub async fn execute_batch_read(
        &self,
        policy: &BatchPolicy,
        batch_reads: Vec<BatchRead>,
    ) -> Result<Vec<BatchRead>> {
        let mut batch_nodes = self.get_batch_nodes(&batch_reads)?;
        let batch_reads = SharedSlice::new(batch_reads);
        let jobs = batch_nodes
            .drain()
            .map(|(node, offsets)| {
                BatchReadCommand::new(policy, node, batch_reads.clone(), offsets)
            })
            .collect();
        self.execute_batch_jobs(jobs, &policy.concurrency).await?;
        batch_reads.into_inner()
    }

    async fn execute_batch_jobs(
        &self,
        jobs: Vec<BatchReadCommand>,
        concurrency: &Concurrency,
    ) -> Result<()> {
        let max_in_flight = match *concurrency {
            Concurrency::Sequential => 1,
            Concurrency::Parallel => jobs.len(),
            Concurrency::MaxThreads(max) => cmp::min(max, jobs.len()),
        };

        // this is not true thread-level concurrency, just concurrency of in-flight futures...
        iter(jobs)
            .map(|mut job| {
                tokio::spawn(async move { job.execute().await })
            })
            .buffer_unordered(max_in_flight)
            .fold(Ok(()), |last_result, result| async {
                match result {
                    Ok(Ok(())) => last_result,
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(e.into()),
                }
            }).await
    }

    fn get_batch_nodes(
        &self,
        batch_reads: &[BatchRead],
    ) -> Result<HashMap<Arc<Node>, Vec<usize>>> {
        let mut map = HashMap::new();
        for (idx, batch_read) in batch_reads.iter().enumerate() {
            let node = self.node_for_key(&batch_read.key)?;
            map.entry(node).or_insert_with(|| vec![]).push(idx);
        }
        Ok(map)
    }

    fn node_for_key(&self, key: &Key) -> Result<Arc<Node>> {
        let partition = Partition::new_by_key(key);
        let node = self.cluster.get_node(&partition)?;
        Ok(node)
    }
}

// A slice with interior mutability, that can be shared across threads. The threads are required to
// ensure that no member of the slice is accessed by more than one thread. No runtime checks are
// performed by the slice to guarantee this.
pub struct SharedSlice<T> {
    value: Arc<UnsafeCell<Vec<T>>>,
}

unsafe impl<T> Send for SharedSlice<T> {}

unsafe impl<T> Sync for SharedSlice<T> {}

impl<T> Clone for SharedSlice<T> {
    fn clone(&self) -> Self {
        SharedSlice {
            value: self.value.clone(),
        }
    }
}

impl<T> SharedSlice<T> {
    pub fn new(value: Vec<T>) -> Self {
        SharedSlice {
            value: Arc::new(UnsafeCell::new(value)),
        }
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        unsafe { (*self.value.get()).get(idx) }
    }

    // Like slice.get_mut but does not require a mutable reference!
    pub fn get_mut(&self, idx: usize) -> Option<&mut T> {
        unsafe { (&mut *self.value.get()).get_mut(idx) }
    }

    pub fn len(&self) -> usize {
        unsafe { (*self.value.get()).len() }
    }

    pub fn into_inner(self) -> Result<Vec<T>> {
        match Arc::try_unwrap(self.value) {
            Ok(cell) => Ok(cell.into_inner()),
            Err(_) => Err("Unable to process batch request".into()),
        }
    }
}
