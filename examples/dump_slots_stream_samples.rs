use std::collections::HashMap;
///
/// get a sample stream of slots with slot number, parent and status to test logic of chain_data with that
///

use log::{info, warn};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::env;

use base64::Engine;
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
use tokio::sync::mpsc::Receiver;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots, SubscribeUpdateBlock};

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use tokio::time::{sleep, Duration};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

fn start_slot_multi_consumer(mut slots_channel: Receiver<Message>) {
    tokio::spawn(async move {
        loop {
            match slots_channel.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Slot(meta)) => {
                        info!("emitted slot #{} from multiplexer", meta.slot);
                    }
                    None => {}
                    _ => {}
                },
                None => {
                    warn!("multiplexer channel closed - aborting");
                    return;
                }
                Some(Message::Connecting(_)) => {}
            }
        }
    });
}


#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using green on {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };
    let (_, exit_notify) = tokio::sync::broadcast::channel(1);

    let green_config =
        GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, None, timeouts.clone());

    let (autoconnect_tx, blockmeta_rx) = tokio::sync::mpsc::channel(10);
    let _green_stream_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        slots_all_confirmation_levels(),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    start_slot_multi_consumer(blockmeta_rx);


    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}


fn slots_all_confirmation_levels() -> SubscribeRequest {
    let mut slot_subs = HashMap::new();
    slot_subs.insert("client".to_string(), SubscribeRequestFilterSlots {
        // implies all slots
        filter_by_commitment: None,
    });
    SubscribeRequest {
        slots: slot_subs,
        ping: None,
        commitment: None,
        ..Default::default()
    }
}
