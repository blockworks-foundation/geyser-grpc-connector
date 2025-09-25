/// logs first shred of slot seen from gRPC - used to compare with validator block production (from logs)

use anyhow::anyhow;
use clap::Parser;
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{
    map_commitment_level, GrpcConnectionTimeouts, GrpcSourceConfig, Message,
};
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::env;
use std::time::Duration;
use solana_sdk::clock::Slot;
use tokio::sync::broadcast;
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{CommitmentLevel as yCL, SlotStatus, SubscribeUpdateSlot};
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterSlots};


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let _args = Args::parse();

    let grpc_addr = env::var("GRPC_ADDR").expect("need grpc url");
    let grpc_x_token = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source {} ({})",
        grpc_addr,
        grpc_x_token.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    let tls_config = ClientTlsConfig::new().with_native_roots();

    let grpc_config = GrpcSourceConfig::new(
        grpc_addr,
        grpc_x_token,
        Some(tls_config.clone()),
        timeouts.clone(),
    );

    let (autoconnect_tx, mut slots_rx) = tokio::sync::mpsc::channel(10);

    let (_exit, exit_notify) = broadcast::channel(1);

    let _ah = create_geyser_autoconnection_task_with_mpsc(
        grpc_config.clone(),
        build_slot_subscription(),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    '_recv_loop: loop {
        match slots_rx.recv().await {
            Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                Some(UpdateOneof::Slot(update_msg)) => {


                    if update_msg.status == SlotStatus::SlotFirstShredReceived as i32 {
                        info!("FIRST_SHRED:{}", update_msg.slot)
                    }


                }
                Some(_) => {}
                None => {}
            },
            None => {
                log::warn!("grpd channel closed - aborting");
                return;
            }
            Some(Message::Connecting(_)) => {}
        }
    }
}

fn build_slot_subscription() -> SubscribeRequest {
    let slots_sub = HashMap::from([(
        "geyser_tracker_slots_all_levels".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: Some(true),
        },
    )]);

    SubscribeRequest {
        slots: slots_sub,
        ..Default::default()
    }
}

