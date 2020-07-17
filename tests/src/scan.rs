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

use std::sync::Arc;
use std::convert::identity;

use crate::common;
use env_logger;

use aerospike::{Client, Bins, ScanPolicy, WritePolicy, as_key, as_bin};
use futures::{future::ready, stream::StreamExt};
use tokio::stream::iter;

const EXPECTED: usize = 1000;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![&wbin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

#[test]
fn scan_single_consumer() {
    let _ = env_logger::try_init();

    common::run_on_current_thread(async {
        let client = common::client().await.unwrap();
        let namespace = common::namespace();
        let set_name = create_test_set(&client, EXPECTED).await;

        let spolicy = ScanPolicy::default();    
        let rs = client
            .scan(&spolicy, namespace, &set_name, Bins::All)
            .await
            .unwrap();

        let count = rs.filter(|r| ready(r.is_ok())).fold(0, |acc, _| async move { acc + 1 }).await;
        assert_eq!(count, EXPECTED);
    });
}

#[test]
fn scan_node() {
    let _ = env_logger::try_init();

    common::run_on_current_thread(async {
        let client = Arc::new(common::client().await.unwrap());
        let namespace = common::namespace();
        let set_name = create_test_set(&client, EXPECTED).await;

        let mut tasks = vec![];

        for node in client.nodes() {
            println!("node: {}", node);
            let client = client.clone();
            let set_name = set_name.clone();
            tasks.push(tokio::spawn(async move {
                let spolicy = ScanPolicy::default();
                client.scan_node(&spolicy, node, namespace, &set_name, Bins::All).await
            }));
        }

        let count: usize = iter(tasks)
                        .filter_map(|t| async move { t.await.ok().and_then(|r| r.ok()) })
                        .flat_map(identity)
                        .filter_map(|r| async move { r.ok().map(|_| 1) })
                        .collect::<Vec<_>>().await
                        .iter()
                        .sum();

        assert_eq!(count, EXPECTED);
    });
}
