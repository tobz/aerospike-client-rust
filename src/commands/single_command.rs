// Copyright 2015-2018 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Instant;

use tokio::time::delay_for;
use error_chain::bail;
use tracing::warn;

use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::{self};
use crate::errors::{ErrorKind, Result, ResultExt};
use crate::net::Connection;
use crate::policy::Policy;
use crate::Key;

pub struct SingleCommand<'a> {
    cluster: Arc<Cluster>,
    pub key: &'a Key,
    partition: Partition<'a>,
}

impl<'a> SingleCommand<'a> {
    pub fn new(cluster: Arc<Cluster>, key: &'a Key) -> Self {
        let partition = Partition::new_by_key(key);
        SingleCommand {
            cluster,
            key,
            partition,
        }
    }

    pub fn get_node(&self) -> Result<Arc<Node>> {
        self.cluster.get_node(&self.partition)
    }

    pub async fn empty_socket(conn: &mut Connection) -> Result<()> {
        // There should not be any more bytes.
        // Empty the socket to be safe.
        let sz = conn.buffer.read_i64(None)?;
        let header_length = i64::from(conn.buffer.read_u8(None)?);
        let receive_size = ((sz & 0xFFFF_FFFF_FFFF) - header_length) as usize;

        // Read remaining message bytes.
        if receive_size > 0 {
            conn.buffer.resize_buffer(receive_size)?;
            conn.read_buffer(receive_size).await?;
        }

        Ok(())
    }

    pub async fn execute<P, C>(policy: P, cmd: &mut C) -> Result<()>
    where
        P: Policy + Send + Sync,
        C: commands::Command,
    {
        let mut iterations = 0;

        // set timeout outside the loop
        let timeout = policy.timeout();
        let deadline = policy.deadline();
        let between = policy.sleep_between_retries();

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            iterations += 1;

            // too many retries
            if let Some(max_retries) = policy.max_retries() {
                if iterations > max_retries + 1 {
                    bail!(ErrorKind::Connection(format!(
                        "Timeout after {} tries",
                        iterations
                    )));
                }
            }

            // Sleep before trying again, after the first iteration
            if iterations > 1 {
                if let Some(dur) = between {
                    println!("sleeping for retry");
                    delay_for(dur).await;
                }
            }

            // check for command timeout
            if let Some(deadline) = deadline {
                if Instant::now() > deadline {
                    break;
                }
            }

            // set command node, so when you return a record it has the node
            let node = match cmd.get_node() {
                Ok(node) => node,
                _ => {
                    println!("cmd node unavailable");
                    continue
                }, // Node is currently inactive. Retry.
            };

            let mut conn = match node.get_connection(timeout).await {
                Ok(conn) => conn,
                Err(err) => {
                    println!("failed to get node connection");
                    warn!("Node {}: {}", node, err);
                    continue;
                }
            };

            cmd.prepare_buffer(&mut conn)
                .chain_err(|| "Failed to prepare send buffer")?;
            cmd.write_timeout(&mut conn, policy.timeout())
                .chain_err(|| "Failed to set timeout for send buffer")?;

            println!("send command iter={}", iterations);
            // Send command.
            if let Err(err) = cmd.write_buffer(&mut conn).await {
                // IO errors are considered temporary anomalies. Retry.
                // Close socket to flush out possible garbage. Do not put back in pool.
                node.invalidate_connection(conn);
                warn!("Node {}: {}", node, err);
                println!("retrying due to error on write_buffer");
                continue;
            }

            println!("parse results iterations={}", iterations);
            // Parse results.
            if let Err(err) = cmd.parse_result(&mut conn).await {
                // close the connection
                // cancelling/closing the batch/multi commands will return an error, which will
                // close the connection to throw away its data and signal the server about the
                // situation. We will not put back the connection in the buffer.
                if !commands::keep_connection(&err) {
                    node.invalidate_connection(conn);
                }
                return Err(err);
            }

            // command has completed successfully.  Exit method.
            return Ok(());
        }

        bail!(ErrorKind::Connection("Timeout".to_string()))
    }
}
