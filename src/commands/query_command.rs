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
use std::time::Duration;

use crate::cluster::Node;
use crate::commands::{Command, SingleCommand, StreamCommand};
use crate::errors::Result;
use crate::net::Connection;
use crate::policy::QueryPolicy;
use crate::{Statement, RecordSender};

use async_trait::async_trait;
use rand::{Rng, thread_rng};

pub struct QueryCommand<'a> {
    stream_command: StreamCommand,
    policy: &'a QueryPolicy,
    statement: Arc<Statement>,
    task_id: u64,
}

impl<'a> QueryCommand<'a> {
    pub fn new(
        policy: &'a QueryPolicy,
        node: Arc<Node>,
        statement: Arc<Statement>,
        tx: RecordSender,
    ) -> Self {
        QueryCommand {
            stream_command: StreamCommand::new(node, tx),
            policy,
            statement,
            task_id: thread_rng().gen(),
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait]
impl<'a> Command for QueryCommand<'a> {
    fn write_timeout(&mut self, conn: &mut Connection, timeout: Option<Duration>) -> Result<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_query(
            self.policy,
            &self.statement,
            false,
            self.task_id,
        )
    }

    fn get_node(&self) -> Result<Arc<Node>> {
        self.stream_command.get_node()
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        StreamCommand::parse_result(&mut self.stream_command, conn).await
    }
}
