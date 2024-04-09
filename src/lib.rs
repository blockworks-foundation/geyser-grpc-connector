use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::time::Duration;
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots, SubscribeUpdate};
use yellowstone_grpc_proto::prost::bytes::Bytes;
use yellowstone_grpc_proto::tonic;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;

pub mod channel_plugger;
pub mod grpc_subscription_autoreconnect_streams;
pub mod grpc_subscription_autoreconnect_tasks;
pub mod grpcmultiplex_fastestwins;
mod obfuscate;

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
}

impl Display for GrpcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "grpc_addr {}",
            crate::obfuscate::url_obfuscate_api_token(&self.grpc_addr)
        )
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
        }
    }

    pub fn build_tonic_endpoint(&self) -> tonic::transport::Endpoint {
        let mut endpoint = tonic::transport::Endpoint::from_shared(self.grpc_addr.clone())
            .expect("grpc_addr must be a valid url");
        if let Some(tls_config) = &self.tls_config {
            endpoint = endpoint.tls_config(tls_config.clone()).expect("tls_config must be valid");
        }
        endpoint
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
            slots: HashMap::new(),
            accounts: Default::default(),
            transactions: HashMap::new(),
            entry: Default::default(),
            blocks: blocks_subs,
            blocks_meta: HashMap::new(),
            commitment: Some(map_commitment_level(self.0) as i32),
            accounts_data_slice: Default::default(),
            ping: None,
        }
    }

    pub fn blocks_meta(&self) -> SubscribeRequest {
        let mut blocksmeta_subs = HashMap::new();
        blocksmeta_subs.insert("client".to_string(), SubscribeRequestFilterBlocksMeta {});

        SubscribeRequest {
            slots: HashMap::new(),
            accounts: Default::default(),
            transactions: HashMap::new(),
            entry: Default::default(),
            blocks: HashMap::new(),
            blocks_meta: blocksmeta_subs,
            commitment: Some(map_commitment_level(self.0) as i32),
            accounts_data_slice: Default::default(),
            ping: None,
        }
    }

    pub fn slots(&self) -> SubscribeRequest {
        let mut slots_subs = HashMap::new();
        slots_subs.insert(
            "client".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
            },
        );

        SubscribeRequest {
            slots: slots_subs,
            accounts: Default::default(),
            transactions: HashMap::new(),
            entry: Default::default(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: Some(map_commitment_level(self.0) as i32),
            accounts_data_slice: Default::default(),
            ping: None,
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
            },
        );

        SubscribeRequest {
            slots: HashMap::new(),
            accounts: accounts_subs,
            transactions: HashMap::new(),
            entry: Default::default(),
            blocks: Default::default(),
            blocks_meta: HashMap::new(),
            commitment: Some(map_commitment_level(self.0) as i32),
            accounts_data_slice: Default::default(),
            ping: None,
        }
    }
}

fn map_commitment_level(commitment_config: CommitmentConfig) -> CommitmentLevel {
    // solana_sdk -> yellowstone
    match commitment_config.commitment {
        solana_sdk::commitment_config::CommitmentLevel::Processed => {
            yellowstone_grpc_proto::prelude::CommitmentLevel::Processed
        }
        solana_sdk::commitment_config::CommitmentLevel::Confirmed => {
            yellowstone_grpc_proto::prelude::CommitmentLevel::Confirmed
        }
        solana_sdk::commitment_config::CommitmentLevel::Finalized => {
            yellowstone_grpc_proto::prelude::CommitmentLevel::Finalized
        }
        _ => {
            panic!(
                "unsupported commitment level {}",
                commitment_config.commitment
            )
        }
    }
}
