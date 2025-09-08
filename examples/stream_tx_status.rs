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
use yellowstone_grpc_proto::geyser::{SlotStatus, SubscribeRequest, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions};

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
    let grpc_addr_blue = env::var("GRPC_ADDR2").expect("need grpc url for blue");
    let grpc_x_token_blue = env::var("GRPC_X_TOKEN2").ok();

    info!(
        "Using grpc source green on {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );
    info!(
        "Using grpc source blue on {} ({})",
        grpc_addr_blue,
        grpc_x_token_blue.is_some()
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
    let blue_config =
        GrpcSourceConfig::new(grpc_addr_blue, grpc_x_token_blue, Some(tls_config), timeouts.clone());

    let (autoconnect_tx, mut slots_rx) = tokio::sync::mpsc::channel(10);

    let (_exit, exit_notify) = broadcast::channel(1);

    let _green_stream_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        build_tx_status_subscription(),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    let _blue_stream_ah = create_geyser_autoconnection_task_with_mpsc(
        blue_config.clone(),
        build_tx_status_subscription(),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    'recv_loop: loop {
        match slots_rx.recv().await {
            Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                Some(UpdateOneof::TransactionStatus(msg)) => {

                    let sig = Signature::try_from(msg.signature.as_slice()).unwrap();

                    info!(
                        "Received tx status: slot {}, status: {:?}",
                        msg.slot,
                        sig
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


fn build_tx_status_subscription() -> SubscribeRequest {
    let mut transactions_status_subs = HashMap::new();
    transactions_status_subs.insert("client".to_string(),
    SubscribeRequestFilterTransactions {
            vote: Some(false),
            // include failed tx as we shouldn't send them again
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
    });

    SubscribeRequest {
        transactions_status: transactions_status_subs,
        commitment: Some(map_commitment_level(CommitmentConfig::confirmed()) as i32),
        ..Default::default()
    }
}

