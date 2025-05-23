use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use solana_sdk::commitment_config::CommitmentConfig;
use tonic::codec::CompressionEncoding;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
    SubscribeUpdate,
};
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;

use crate::obfuscate::url_obfuscate_api_token;
pub use yellowstone_grpc_client::{
    GeyserGrpcClient, GeyserGrpcClientError, GeyserGrpcClientResult,
};

pub mod channel_plugger;
pub mod grpc_subscription_autoreconnect_streams;
pub mod grpc_subscription_autoreconnect_tasks;
pub mod grpcmultiplex_fastestwins;
pub mod histogram_percentiles;
mod obfuscate;
pub mod yellowstone_grpc_util;

pub type AtomicSlot = Arc<AtomicU64>;

// 1-based attempt counter
type Attempt = u32;

// wraps payload and status messages
// clone is required by broacast channel
#[derive(Clone)]
pub enum Message {
    GeyserSubscribeUpdate(Box<SubscribeUpdate>),
    // connect (attempt=1) or reconnect(attempt=2..)
    Connecting(Attempt),
}

#[derive(Clone, Debug)]
pub struct GrpcConnectionTimeouts {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub subscribe_timeout: Duration,
    pub receive_timeout: Duration,
}

#[derive(Clone)]
pub struct GrpcSourceConfig {
    pub grpc_addr: String,
    pub grpc_x_token: Option<String>,
    tls_config: Option<ClientTlsConfig>,
    timeouts: Option<GrpcConnectionTimeouts>,
    compression: Option<CompressionEncoding>,
}

impl Display for GrpcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "grpc_addr {} (token? {}, compression {})",
            url_obfuscate_api_token(&self.grpc_addr),
            if self.grpc_x_token.is_some() {
                "yes"
            } else {
                "no"
            },
            self.compression
                .as_ref()
                .map(|c| c.to_string())
                .unwrap_or("none".to_string())
        )?;

        Ok(())
    }
}

impl Debug for GrpcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl GrpcSourceConfig {
    /// Create a grpc source without tls and timeouts
    pub fn new_simple(grpc_addr: String) -> Self {
        Self {
            grpc_addr,
            grpc_x_token: None,
            tls_config: None,
            timeouts: None,
            compression: None,
        }
    }
    pub fn new(
        grpc_addr: String,
        grpc_x_token: Option<String>,
        tls_config: Option<ClientTlsConfig>,
        timeouts: GrpcConnectionTimeouts,
    ) -> Self {
        Self {
            grpc_addr,
            grpc_x_token,
            tls_config,
            timeouts: Some(timeouts),
            compression: None,
        }
    }
    pub fn new_compressed(
        grpc_addr: String,
        grpc_x_token: Option<String>,
        tls_config: Option<ClientTlsConfig>,
        timeouts: GrpcConnectionTimeouts,
    ) -> Self {
        Self {
            grpc_addr,
            grpc_x_token,
            tls_config,
            timeouts: Some(timeouts),
            compression: Some(CompressionEncoding::Zstd),
        }
    }
}

#[derive(Clone)]
pub struct GeyserFilter(pub CommitmentConfig);

impl GeyserFilter {
    pub fn blocks_and_txs(&self) -> SubscribeRequest {
        let mut blocks_subs = HashMap::new();
        blocks_subs.insert(
            "client".to_string(),
            SubscribeRequestFilterBlocks {
                account_include: Default::default(),
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(false),
            },
        );

        SubscribeRequest {
            blocks: blocks_subs,
            commitment: Some(map_commitment_level(self.0) as i32),
            ..Default::default()
        }
    }

    pub fn blocks_meta(&self) -> SubscribeRequest {
        let mut blocksmeta_subs = HashMap::new();
        blocksmeta_subs.insert("client".to_string(), SubscribeRequestFilterBlocksMeta {});

        SubscribeRequest {
            blocks_meta: blocksmeta_subs,
            commitment: Some(map_commitment_level(self.0) as i32),
            ..Default::default()
        }
    }

    pub fn slots(&self) -> SubscribeRequest {
        let mut slots_subs = HashMap::new();
        slots_subs.insert(
            "client".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            },
        );

        SubscribeRequest {
            slots: slots_subs,
            commitment: Some(map_commitment_level(self.0) as i32),
            ..Default::default()
        }
    }

    pub fn accounts(&self) -> SubscribeRequest {
        let mut accounts_subs = HashMap::new();
        accounts_subs.insert(
            "client".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
            },
        );

        SubscribeRequest {
            accounts: accounts_subs,
            commitment: Some(map_commitment_level(self.0) as i32),
            ..Default::default()
        }
    }
}

pub fn map_commitment_level(commitment_config: CommitmentConfig) -> CommitmentLevel {
    // solana_sdk -> yellowstone
    match commitment_config.commitment {
        solana_sdk::commitment_config::CommitmentLevel::Processed => CommitmentLevel::Processed,
        solana_sdk::commitment_config::CommitmentLevel::Confirmed => CommitmentLevel::Confirmed,
        solana_sdk::commitment_config::CommitmentLevel::Finalized => CommitmentLevel::Finalized,
    }
}
