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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::common;
use env_logger;

use aerospike::*;
use tokio::time::delay_for;
use tokio::stream::StreamExt;

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

    // create an index
    client
        .create_index(
            &wpolicy,
            namespace,
            &set_name,
            "bin",
            &format!("{}_{}_{}", namespace, set_name, "bin"),
            IndexType::Numeric,
        )
        .await
        .expect("Failed to create index");

    delay_for(Duration::from_millis(3000)).await;

    set_name
}

#[test]
fn query_single_consumer() {
    let _ = env_logger::try_init();

    common::run_on_current_thread(async {
        let client = common::client().await.unwrap();
        let namespace = common::namespace();
        let set_name = create_test_set(&client, EXPECTED).await;
        let qpolicy = QueryPolicy::default();

        // Filter Query
        let mut statement = Statement::new(namespace, &set_name, Bins::All);
        statement.add_filter(as_eq!("bin", 1));
        let mut rs = client.query(&qpolicy, statement).await.unwrap();
        let mut count = 0;
        while let Some(res) = rs.next().await {
            match res {
                Ok(rec) => {
                    assert_eq!(rec.bins["bin"], as_val!(1));
                    count += 1;
                }
                Err(err) => panic!(format!("{:?}", err)),
            }
        }
        assert_eq!(count, 1);

        // Range Query
        let mut statement = Statement::new(namespace, &set_name, Bins::All);
        statement.add_filter(as_range!("bin", 0, 9));
        let mut rs = client.query(&qpolicy, statement).await.unwrap();
        let mut count = 0;
        while let Some(res) = rs.next().await {
            match res {
                Ok(rec) => {
                    count += 1;
                    let v: i64 = rec.bins["bin"].clone().into();
                    assert!(v >= 0);
                    assert!(v < 10);
                }
                Err(err) => panic!(format!("{:?}", err)),
            }
        }
        assert_eq!(count, 10);
    });
}

#[test]
fn query_nobins() {
    let _ = env_logger::try_init();

    common::run_on_current_thread(async {
        let client = common::client().await.unwrap();
        let namespace = common::namespace();
        let set_name = create_test_set(&client, EXPECTED).await;
        let qpolicy = QueryPolicy::default();

        let mut statement = Statement::new(namespace, &set_name, Bins::None);
        statement.add_filter(as_range!("bin", 0, 9));
        let mut rs = client.query(&qpolicy, statement).await.unwrap();
        let mut count = 0;
        while let Some(res) = rs.next().await {
            match res {
                Ok(rec) => {
                    count += 1;
                    assert!(rec.generation > 0);
                    assert_eq!(0, rec.bins.len());
                }
                Err(err) => panic!(format!("{:?}", err)),
            }
        }
        assert_eq!(count, 10);
    });
}

#[test]
fn query_node() {
    let _ = env_logger::try_init();

    common::run_on_current_thread(async {
        let client = Arc::new(common::client().await.unwrap());
        let namespace = common::namespace();
        let set_name = create_test_set(&client, EXPECTED).await;

        let count = Arc::new(AtomicUsize::new(0));
        let mut tasks = vec![];

        for node in client.nodes() {
            let client = client.clone();
            let count = count.clone();
            let set_name = set_name.clone();
            tasks.push(tokio::spawn(async move {
                let qpolicy = QueryPolicy::default();
                let mut statement = Statement::new(namespace, &set_name, Bins::All);
                statement.add_filter(as_range!("bin", 0, 99));
                
                match client.query_node(&qpolicy, node, statement).await {
                    Ok(rs) => {
                        let n = rs.filter(Result::is_ok).fold(0, |acc, _| acc + 1).await;
                        count.fetch_add(n, Ordering::Relaxed);
                        Ok(n)
                    },
                    Err(e) => {
                        println!("failed to query node: {:?}", e);
                        Err(e)
                    },
                }
            }));
        }

        for t in tasks {
            let _ = t.await.expect("failed to join tasks");
        }

        assert_eq!(count.load(Ordering::Relaxed), 100);
    });
}
