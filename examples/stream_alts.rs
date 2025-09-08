use clap::Parser;
use log::info;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::time::Duration;
use anyhow::Context;
use solana_sdk::clock::Slot;
use solana_sdk::signature::Signature;
use tokio::sync::broadcast;
use tokio::time::Instant;
use tonic::transport::ClientTlsConfig;
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{
    map_commitment_level, GrpcConnectionTimeouts, GrpcSourceConfig, Message,
};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SlotStatus, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions};
use yellowstone_grpc_proto::prost::Message as ProtoMessage;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source green on {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    let tls_config = ClientTlsConfig::new().with_native_roots();

    let green_config =
        GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, Some(tls_config.clone()), timeouts.clone());

    let (autoconnect_tx, mut slots_rx) = tokio::sync::mpsc::channel(10);

    let (_exit, exit_notify) = broadcast::channel(1);

    let _green_stream_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        build_alt_subscription(),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    'recv_loop: loop {
        match slots_rx.recv().await {
            Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                Some(UpdateOneof::Account(msg)) => {

                    info!(
                        "Received alt message: slot {}, msgsize {}",
                        msg.slot,
                        msg.encoded_len()
                    );
                }
                Some(_) => {}
                None => {}
            },
            None => {
                log::warn!("multiplexer channel closed - aborting");
                return;
            }
            Some(Message::Connecting(_)) => {}
        }
    }
}


fn build_alt_subscription() -> SubscribeRequest {
    let mut accounts_subs = HashMap::new();

    accounts_subs.insert(
        "accounts".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec!["AddressLookupTab1e1111111111111111111111111".to_string()],
            filters: vec![],
            nonempty_txn_signature: None,
        },
    );

    SubscribeRequest {
        accounts: accounts_subs,
        commitment: Some(map_commitment_level(CommitmentConfig::confirmed()) as i32),
        ..Default::default()
    }
}

