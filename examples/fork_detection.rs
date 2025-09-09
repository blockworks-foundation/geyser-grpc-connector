use clap::Parser;
use log::{info, warn};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel as solanaCL, CommitmentLevel};
use yellowstone_grpc_proto::geyser::{CommitmentLevel as yCL, SubscribeUpdateSlot};
use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;
use std::time::Duration;
use anyhow::{anyhow, Context};
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

// 2025-09-09T16:12:30.236552Z  INFO fork_detection: Fork-Detection: Slot 365713185 finalized, parent=365713184
// 2025-09-09T16:12:30.661459Z  INFO fork_detection: Fork-Detection: Slot 365713186 finalized, parent=365713185
// 2025-09-09T16:12:30.661600Z  INFO fork_detection: Fork-Detection: Slot 365713187 finalized, parent=365713186
// 2025-09-09T16:12:30.661642Z  WARN fork_detection: Fork-Detection: Slot 365713192 finalized, parent=365713187
// 2025-09-09T16:12:30.661669Z  INFO fork_detection: checking slot 365713188, orphan=true
// 2025-09-09T16:12:30.661692Z  INFO fork_detection: checking slot 365713189, orphan=true
// 2025-09-09T16:12:30.661714Z  INFO fork_detection: checking slot 365713190, orphan=false
// 2025-09-09T16:12:30.661736Z  INFO fork_detection: checking slot 365713191, orphan=false
// 2025-09-09T16:12:31.170278Z  INFO fork_detection: Fork-Detection: Slot 365713193 finalized, parent=365713192
// 2025-09-09T16:12:31.484739Z  INFO fork_detection: Fork-Detection: Slot 365713194 finalized, parent=365713193
// 2025-09-09T16:12:31.986628Z  INFO fork_detection: Fork-Detection: Slot 365713195 finalized, parent=365713194
// 2025-09-09T16:12:32.282026Z  INFO fork_detection: Fork-Detection: Slot 365713196 finalized, parent=365713195
// 2025-09-09T16:12:32.754367Z  INFO fork_detection: Fork-Detection: Slot 365713197 finalized, parent=365713196

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {


}

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

    let grpc_config =
        GrpcSourceConfig::new(grpc_addr, grpc_x_token, Some(tls_config.clone()), timeouts.clone());

    let (autoconnect_tx, mut slots_rx) = tokio::sync::mpsc::channel(10);

    let (_exit, exit_notify) = broadcast::channel(1);

    let _ah = create_geyser_autoconnection_task_with_mpsc(
        grpc_config.clone(),
        build_slot_subscription(),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    let mut all_slots: HashSet<Slot> = HashSet::with_capacity(1024);
    'recv_loop: loop {
        match slots_rx.recv().await {
            Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                Some(UpdateOneof::Slot(update_msg)) => {

                    let slot_status = map_slot_status_to_commitment_level(&update_msg)
                        .unwrap_or_else(|_| {
                            panic!("invalid commitment level: status={}", update_msg.status)
                        });

                    if slot_status == Some(solanaCL::Processed) {
                        all_slots.insert(update_msg.slot);
                    }

                    if slot_status == Some(solanaCL::Finalized) {

                        let last_finalized_slot = update_msg.parent.expect("parent slot");
                        let diff = update_msg.slot - last_finalized_slot;
                        if diff == 1 {
                            info!("Fork-Detection: Slot {} finalized, parent={}", update_msg.slot, last_finalized_slot);
                        } else if diff > 1 {
                            warn!("Fork-Detection: Slot {} finalized, parent={}", update_msg.slot, last_finalized_slot);
                        }

                        for checking_slot in (last_finalized_slot+1)..=(update_msg.slot-1) {
                            let orphan_slot_seen = all_slots.contains(&checking_slot);
                            info!("checking slot {}, orphan={}", checking_slot, orphan_slot_seen);
                        }
                    }

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


fn build_slot_subscription() -> SubscribeRequest {
    let slots_sub = HashMap::from([(
        "geyser_tracker_slots_all_levels".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            // do not send "new" slot status like FirstShredReceived but only Processed, Confirmed, Finalized
            interslot_updates: Some(false),
        },
    )]);

    SubscribeRequest {
        slots: slots_sub,
        commitment: Some(map_commitment_level(CommitmentConfig::processed()) as i32),
        ..Default::default()
    }
}

fn map_slot_status_to_commitment_level(
    slot_update: &SubscribeUpdateSlot,
) -> anyhow::Result<Option<CommitmentLevel>> {
    yCL::try_from(slot_update.status)
        .map(|v| match v {
            yCL::Processed => Some(solanaCL::Processed),
            yCL::Confirmed => Some(solanaCL::Confirmed),
            yCL::Finalized => Some(solanaCL::Finalized),
        })
        .map_err(|_| anyhow!("invalid commitment level"))
}