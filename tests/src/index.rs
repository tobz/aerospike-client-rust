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

use std::time::Duration;

use crate::common;
use env_logger;

use aerospike::*;
use tokio::time::delay_for;

const EXPECTED: usize = 100;

#[test]
#[should_panic(expected = "IndexFound")]
fn recreate_index() {
    let _ = env_logger::try_init();

    common::run_on_current_thread(async {
        let client = common::client().await.unwrap();
        let ns = common::namespace();
        let set = common::rand_str(10);
        let policy = WritePolicy::default();
        for i in 0..EXPECTED as i64 {
            let key = as_key!(ns, &set, i);
            let wbin = as_bin!("bin", i);
            let bins = vec![&wbin];
            client.delete(&policy, &key).await.unwrap();
            client.put(&policy, &key, &bins).await.unwrap();
        }
        delay_for(Duration::from_millis(3000)).await;
        let bin = "bin";
        let index = format!("{}_{}_{}", ns, set, bin);

        let _ = client.drop_index(&policy, ns, &set, &index).await;
        delay_for(Duration::from_millis(1000)).await;

        client
            .create_index(&policy, ns, &set, bin, &index, IndexType::Numeric)
            .await
            .expect("Failed to create index");
        delay_for(Duration::from_millis(1000)).await;

        client
            .create_index(&policy, ns, &set, bin, &index, IndexType::Numeric)
            .await
            .unwrap();
    });
}
