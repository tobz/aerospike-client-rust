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

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use deadpool::managed::{Manager, Pool, PoolError, Object, RecycleResult, RecycleError};

use crate::errors::{Error, ErrorKind};
use crate::net::{Connection, Host};
use crate::policy::ClientPolicy;

struct PoolManager {
    host: Host,
    policy: ClientPolicy,
}

pub struct ConnectionPool(Pool<Connection, Error>);

impl ConnectionPool {
    pub fn new(host: Host, policy: ClientPolicy) -> Self {
        let capacity = policy.max_conns_per_node;
        let manager = PoolManager {
            host,
            policy,
        };
        ConnectionPool(Pool::new(manager, capacity))
    }

    pub async fn get(&self, timeout: Option<Duration>) -> Result<Object<Connection, Error>, Error> {
        self.0.get().await
            .map_err(|e| match e {
                PoolError::Timeout(_) => ErrorKind::PoolTimedOut.into(),
                PoolError::Backend(e2) => e2,
            })
            .map(|mut conn| {
                conn.set_timeout(timeout);
                conn
            })
    }

    pub fn invalidate(&self, connection: Object<Connection, Error>) {
        drop(Object::take(connection));
    }

    pub async fn close(&self) {
        // Drain all connections from the pool until there are no more.
        while let Ok(conn) = self.0.try_get().await {
            self.invalidate(conn);
        }
    }
}

impl fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = self.0.status();
        f.debug_struct("ConnectionPool")
         .field("available_conns", &status.available)
         .field("current_conns", &status.size)
         .field("max_conns", &status.max_size)
         .finish()
    }
}

#[async_trait]
impl Manager<Connection, Error> for PoolManager {
    async fn create(&self) -> Result<Connection, Error> {
        Connection::new(self.host.as_socket_addr(), &self.policy).await
    }

    async fn recycle(&self, conn: &mut Connection) -> RecycleResult<Error> {
        if conn.is_idle() {
            return Err(RecycleError::Backend(ErrorKind::ConnectionPastIdleDeadline.into()))
        }

        Ok(())
    }
}