use log::{info, warn};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::time::SystemTime;

use base64::Engine;
use csv::ReaderBuilder;
use itertools::Itertools;
use solana_sdk::borsh0_10::try_from_slice_unchecked;
/// This file mocks the core model of the RPC server.
use solana_sdk::compute_budget;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::message::v0::MessageAddressTableLookup;
use solana_sdk::message::{v0, MessageHeader, VersionedMessage};
use solana_sdk::pubkey::Pubkey;

use solana_sdk::signature::Signature;
use solana_sdk::transaction::TransactionError;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Receiver;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    SubscribeUpdateSlot,
};

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::{
    map_commitment_level, GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, Message,
};
use tokio::time::{sleep, Duration};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccounts;

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();

    let (_foo, exit_notify) = broadcast::channel(1);

    info!(
        "Using gRPC source {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    let green_config =
        GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, None, timeouts.clone());

    let (autoconnect_tx, mut transactions_rx) = tokio::sync::mpsc::channel(10);
    let _tx_source_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        jupyter_trades(),
        autoconnect_tx.clone(),
        exit_notify,
    );

    loop {
        let message = transactions_rx.recv().await;
        match message {
            Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                Some(UpdateOneof::Transaction(update)) => {
                    let tx = update.transaction.unwrap();
                    let sig = Signature::try_from(tx.signature.as_slice()).unwrap();
                    info!("tx {}", sig);
                }
                _ => {} // FIXME
            },
            _ => {} // FIXME
        }
    }
}

fn jupyter_trades() -> SubscribeRequest {
    let mut transaction_subs = HashMap::new();
    transaction_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec!["JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string()],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeRequest {
        transactions: transaction_subs,
        ping: None,
        commitment: Some(map_commitment_level(CommitmentConfig {
            commitment: CommitmentLevel::Confirmed,
        }) as i32),
        ..Default::default()
    }
}
