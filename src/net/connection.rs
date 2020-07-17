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

use std::net::Shutdown;
use tokio::select;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::time::{delay_for, timeout};
use std::ops::Add;
use std::time::{Duration, Instant};

use crate::commands::admin_command::AdminCommand;
use crate::commands::buffer::Buffer;
use crate::errors::{Result, ErrorKind};
use crate::policy::ClientPolicy;

use rand::{thread_rng, Rng};

#[derive(Debug)]
pub struct Connection {
    conn_id: u64,

    timeout: Option<Duration>,

    // duration after which connection is considered idle
    idle_timeout: Option<Duration>,
    idle_deadline: Option<Instant>,

    // connection object
    conn: BufStream<TcpStream>,

    bytes_read: usize,

    pub buffer: Buffer,
}

impl Connection {
    pub async fn new<T: ToSocketAddrs>(addr: T, policy: &ClientPolicy) -> Result<Self> {
        let connect_timeout = policy.connect_timeout
            .unwrap_or(Duration::new(0, 0));

        let delay = delay_for(connect_timeout);
        let conn = select! { 
            conn = TcpStream::connect(addr) => conn?,
            _ = delay, if policy.connect_timeout.is_some() => {
                return Err(ErrorKind::FailedToConnect.into());
            }
        };
        let mut conn = Connection {
            conn_id: thread_rng().gen(),
            buffer: Buffer::new(),
            bytes_read: 0,
            // we probably need to join a tokio timeout future to each read/write
            // if this is set, and then our interface can abstract the timeouts
            // behind the pseudo block-y API
            timeout: None,
            conn: BufStream::new(conn),
            idle_timeout: policy.idle_timeout,
            idle_deadline: match policy.idle_timeout {
                None => None,
                Some(timeout) => Some(Instant::now() + timeout),
            },
        };
        conn.authenticate(&policy.user_password).await?;
        conn.refresh();
        Ok(conn)
    }

    pub fn close(&mut self) {
        let _ = self.conn.get_ref().shutdown(Shutdown::Both);
    }

    pub async fn flush(&mut self) -> Result<()> {
        let op_timeout = self.timeout;
        let op = async {
            self.conn.write_all(&self.buffer.data_buffer).await?;
            self.conn.flush().await
        };
        match op_timeout {
            None => op.await?,
            Some(dur) => {
                timeout(dur, op).await
                    .map_err(|_| io_timeout())
                    .and_then(|inner| inner)?
            }
        }
        self.refresh();
        Ok(())
    }

    pub async fn read_buffer(&mut self, size: usize) -> Result<()> {
        self.buffer.resize_buffer(size)?;
        let op = self.conn.read_exact(&mut self.buffer.data_buffer);
        match self.timeout {
            None => {
                op.await?;
            },
            Some(dur) => {
                timeout(dur, op).await
                    .map_err(|_| io_timeout())
                    .and_then(|inner| inner)?;
            }
        }
        self.bytes_read += size;
        self.buffer.reset_offset()?;
        self.refresh();
        Ok(())
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<()> {
        let op_timeout = self.timeout;
        let op = async {
            self.conn.write_all(buf).await?;
            self.conn.flush().await
        };
        match op_timeout {
            None => op.await?,
            Some(dur) => {
                timeout(dur, op).await
                    .map_err(|_| io_timeout())
                    .and_then(|inner| inner)?
            }
        }
        self.refresh();
        Ok(())
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<()> {
        let op = self.conn.read_exact(buf);
        match self.timeout {
            None => {
                op.await?;
            },
            Some(dur) => {
                timeout(dur, op).await
                    .map_err(|_| io_timeout())
                    .and_then(|inner| inner)?;
            }
        }
        self.bytes_read += buf.len();
        self.refresh();
        Ok(())
    }

    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
    }

    pub fn is_idle(&self) -> bool {
        if let Some(idle_dl) = self.idle_deadline {
            Instant::now() >= idle_dl
        } else {
            false
        }
    }

    fn refresh(&mut self) {
        self.idle_deadline = None;
        if let Some(idle_to) = self.idle_timeout {
            self.idle_deadline = Some(Instant::now().add(idle_to))
        };
    }

    async fn authenticate(&mut self, user_password: &Option<(String, String)>) -> Result<()> {
        if let Some((ref user, ref password)) = *user_password {
            match AdminCommand::authenticate(self, user, password).await {
                Ok(()) => {
                    return Ok(());
                }
                Err(err) => {
                    self.close();
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    pub fn bookmark(&mut self) {
        self.bytes_read = 0;
    }

    pub const fn bytes_read(&self) -> usize {
        self.bytes_read
    }
}

fn io_timeout() -> std::io::Error {
    std::io::ErrorKind::TimedOut.into()
}