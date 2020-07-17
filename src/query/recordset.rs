// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache Licenseersion 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use std::task::{Context, Poll};
use std::pin::Pin;

use crate::errors::Result;
use crate::record::Record;

use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::stream::Stream;

/// Sender side of a record set.
pub type RecordSender = Sender<Result<Record>>;

/// Set of records returned by a scan/query operation.
pub struct RecordSet(Receiver<Result<Record>>);

impl RecordSet {
    /// Creates a new record set, and sender, with the given capacity.
    pub fn new(capacity: usize) -> (RecordSender, RecordSet) {
        let (tx, rx) = channel(capacity);

        (tx, RecordSet(rx))
    }
}

impl Stream for RecordSet {
    type Item = Result<Record>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        // switch to pin-project at some point
        let rx = unsafe { self.map_unchecked_mut(|this| &mut this.0) };
        rx.poll_next(cx)
    }
}