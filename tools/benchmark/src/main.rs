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

mod cli;
mod generator;
mod percent;
mod stats;
mod workers;

use std::sync::Arc;

use aerospike::{Client, ClientPolicy};
use log::info;
use tokio::runtime::Builder;
use tokio::sync::mpsc;

use cli::Options;
use generator::KeyPartitions;
use stats::Collector;
use workers::Worker;

fn main() {
    let _ = env_logger::try_init();
    let options = cli::parse_options();
    info!("{:?}", options);
    run_workload(options);
}

fn run_workload(opts: Options) {
    let mut rt = Builder::new()
        .threaded_scheduler()
        .enable_all()
        .build()
        .unwrap();
    
    rt.block_on(async {
        let mut policy = ClientPolicy::default();
        policy.conn_pools_per_node = opts.conn_pools_per_node;
        let client = Arc::new(Client::new(&policy, &opts.hosts).await.unwrap());
        let (tx, rx) = mpsc::unbounded_channel();
        let collector = Collector::new(rx);
        for keys in KeyPartitions::new(
            opts.namespace,
            opts.set,
            opts.start_key,
            opts.keys,
            opts.concurrency,
        ) {
            let mut worker = Worker::for_workload(&opts.workload, client.clone(), tx.clone());
            tokio::spawn(async move { worker.run(keys).await });
        }
        drop(tx);
        collector.collect().await;
    });
}
