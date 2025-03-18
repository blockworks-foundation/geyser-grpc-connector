use clap::Parser;
use log::info;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::broadcast;

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{
    map_commitment_level, GrpcConnectionTimeouts, GrpcSourceConfig, Message,
};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub commitment_level: String,
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    // RUST_LOG=info,stream_blocks_processed=debug,geyser_grpc_connector=trace
    tracing_subscriber::fmt::init();
    // console_subscriber::init();

    let args = Args::parse();

    let commitment_level = CommitmentLevel::from_str(&args.commitment_level.to_ascii_lowercase())
        .unwrap_or_else(|_| {
            panic!(
                "Invalid argument commitment level: {}",
                args.commitment_level
            )
        });
    let mut commitment_level_short = commitment_level.to_string().to_ascii_uppercase();
    commitment_level_short.truncate(1);

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source on {} ({})",
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

    info!("Write Block stream..");
    let (autoconnect_tx, mut blocks_rx) = tokio::sync::mpsc::channel(10);

    let (_exit, exit_notify) = broadcast::channel(1);

    let _green_stream_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        build_subscription(commitment_level),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    loop {
        match blocks_rx.recv().await {
            Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                Some(UpdateOneof::Slot(_update_slot)) => {}
                Some(UpdateOneof::Block(update_block)) => {
                    info!(
                        "({}) block {:?}: {} txs",
                        commitment_level_short,
                        update_block.slot,
                        update_block.transactions.len()
                    );
                }
                None => {}
                _ => {}
            },
            None => {
                log::warn!("multiplexer channel closed - aborting");
                return;
            }
            Some(Message::Connecting(_)) => {}
        }
    }
}

fn build_subscription(commitment_level: CommitmentLevel) -> SubscribeRequest {
    let mut slots_subs = HashMap::new();
    slots_subs.insert(
        "geyser_slots".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            interslot_updates: Some(false),
        },
    );

    let mut blocks_subs = HashMap::new();
    blocks_subs.insert(
        "geyser_full_blocks".to_string(),
        SubscribeRequestFilterBlocks {
            account_include: Default::default(),
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(false),
        },
    );

    SubscribeRequest {
        slots: slots_subs,
        blocks: blocks_subs,
        commitment: Some(map_commitment_level(CommitmentConfig {
            commitment: commitment_level,
        }) as i32),
        ..Default::default()
    }
}
