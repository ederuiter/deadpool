//! This module extends the library to support Redis Cluster.
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use redis;
use redis::aio::MultiplexedConnection;
use redis::sentinel::Sentinel;
use redis::{aio::ConnectionLike, AsyncConnectionConfig, ConnectionInfo, IntoConnectionInfo, RedisError, RedisResult};
use tokio::sync::Mutex;

use deadpool::managed;
pub use deadpool::managed::reexports::*;

pub use crate::sentinel::config::SentinelNodeConnectionInfo;
pub use crate::sentinel::config::SentinelServerType;
pub use crate::sentinel::config::TlsMode;

pub use self::config::{Config, ConfigError};

mod config;

deadpool::managed_reexports!(
    "redis_sentinel",
    Manager,
    Connection,
    RedisError,
    ConfigError
);

type RecycleResult = managed::RecycleResult<RedisError>;

/// Wrapper around [`redis::aio::MultiplexedConnection`].
///
/// This structure implements [`redis::aio::ConnectionLike`] and can therefore
/// be used just like a regular [`redis::aio::MultiplexedConnection`].
pub struct Connection {
    conn: Object,
}

impl Connection {
    /// Takes this [`Connection`] from its [`Pool`] permanently.
    ///
    /// This reduces the size of the [`Pool`].
    #[must_use]
    pub fn take(this: Self) -> SentinelConnection {
        Object::take(this.conn)
    }
}

impl From<Object> for Connection {
    fn from(conn: Object) -> Self {
        Self { conn }
    }
}

impl Deref for Connection {
    type Target = SentinelConnection;

    fn deref(&self) -> &SentinelConnection {
        &self.conn
    }
}

impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut SentinelConnection {
        &mut self.conn
    }
}

impl AsRef<SentinelConnection> for Connection {
    fn as_ref(&self) -> &SentinelConnection {
        &self.conn
    }
}

impl AsMut<SentinelConnection> for Connection {
    fn as_mut(&mut self) -> &mut SentinelConnection {
        &mut self.conn
    }
}

impl ConnectionLike for Connection {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        self.conn.conn.req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        self.conn.conn.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.conn.conn.get_db()
    }
}

pub struct SentinelConnection {
    pub conn: MultiplexedConnection,
    pub connection_info: ConnectionInfo
}

/// [`Manager`] for creating and recycling [`redis::aio::MultiplexedConnection`] connections.
///
/// [`Manager`]: managed::Manager
pub struct Manager {
    client: Mutex<Sentinel>,
    service_name: String,
    node_connection_info: SentinelNodeConnectionInfo,
    server_type: SentinelServerType,
    ping_number: AtomicUsize,
}

impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager")
            .field("client", &format!("{:p}", &self.client))
            .field("service_name", &self.service_name)
            .field("node_connection_info", &self.node_connection_info)
            .field("server_type", &self.server_type)
            .field("ping_number", &self.ping_number)
            .finish()
    }
}

impl Manager {
    /// Creates a new [`Manager`] from the given `params`.
    ///
    /// # Errors
    ///
    /// If establishing a new [`Sentinel`] fails.
    pub fn new<T: IntoConnectionInfo>(
        param: Vec<T>,
        service_name: String,
        node_connection_info: Option<SentinelNodeConnectionInfo>,
        server_type: SentinelServerType,
    ) -> RedisResult<Self> {
        Ok(Self {
            client: Mutex::new(Sentinel::build(param)?),
            service_name,
            node_connection_info: node_connection_info.unwrap_or_default(),
            server_type,
            ping_number: AtomicUsize::new(0),
        })
    }
}

impl managed::Manager for Manager {
    type Type = SentinelConnection;
    type Error = RedisError;

    async fn create(&self) -> Result<SentinelConnection, RedisError> {


        let mut client = self.client.lock().await;
        let con = match self.server_type {
            SentinelServerType::Master => {
                client
                    .async_master_for(self.service_name.as_str(), Some(&self.node_connection_info.clone().into()))
                    .await
            }
            SentinelServerType::Replica => {
                client
                    .async_replica_for(self.service_name.as_str(), Some(&self.node_connection_info.clone().into()))
                    .await
            }
        }?;


        let connection_info = con.get_connection_info().clone();
        println!("[redis] created new connection to {}", connection_info.addr.to_string());

        Ok(SentinelConnection {
            connection_info,
            conn: con.get_multiplexed_async_connection_with_config(
                &AsyncConnectionConfig::new()
                    .set_response_timeout(Duration::from_secs(5))
                    .set_connection_timeout(Duration::from_secs(2)),
            ).await?,
        })
    }

    async fn recycle(&self, conn: &mut SentinelConnection, _: &Metrics) -> RecycleResult {
        // let mut client = self.client.lock().await;
        //
        // let con = match self.server_type {
        //     SentinelServerType::Master => {
        //         redis::cmd("SENTINEL")
        //             .arg("get-master-addr-by-name")
        //             .arg(self.service_name.clone())
        //             .query_async(client)
        //     }
        //     SentinelServerType::Replica => {
        //         client
        //             .async_replica_for(self.service_name.as_str(), Some(&self.node_connection_info.clone().into()))
        //             .await
        //     }
        // }?;
        //

        let ping_number = self.ping_number.fetch_add(1, Ordering::Relaxed).to_string();
        println!("[redis] recycled connection to {}", conn.connection_info.addr.to_string());
        let n = redis::cmd("PING")
            .arg(&ping_number)
            .query_async::<String>(&mut conn.conn)
            .await?;
        if n == ping_number {
            Ok(())
        } else {
            Err(managed::RecycleError::message("Invalid PING response"))
        }
    }
}
