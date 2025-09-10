//
// ```
// ssh eclipse-rpc -Nv
// ```
//

use log::{info, trace};
use solana_account_decoder::parse_token::spl_token_ids;
use solana_sdk::clock::{Slot, UnixTimestamp};
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use itertools::Itertools;
use solana_sdk::bs58;
use solana_sdk::commitment_config::CommitmentLevel;
use tokio::sync::mpsc::{Receiver, Sender};

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use tokio::time::{sleep, Duration};
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdateSlot};

type AtomicSlot = Arc<AtomicU64>;

#[tokio::main]
pub async fn main() {
    // RUST_LOG=info,stream_blocks_mainnet=debug,geyser_grpc_connector=trace
    tracing_subscriber::fmt::init();
    // console_subscriber::init();

    let grpc_addr = env::var("GRPC_ADDR").expect("need grpc url");
    let grpc_x_token = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source on {} ({})",
        grpc_addr,
        grpc_x_token.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(25),
        request_timeout: Duration::from_secs(25),
        subscribe_timeout: Duration::from_secs(25),
        receive_timeout: Duration::from_secs(25),
    };

    let tls_config = ClientTlsConfig::new().with_native_roots();
    let config = GrpcSourceConfig::new(grpc_addr, grpc_x_token, Some(tls_config), timeouts.clone());

    let (autoconnect_tx, geyser_messages_rx) = tokio::sync::mpsc::channel(10);
    let (_exit_tx, exit_rx) = tokio::sync::broadcast::channel::<()>(1);
    let (subscribe_filter_update_tx, mut _subscribe_filter_update_rx) =
        tokio::sync::mpsc::channel::<SubscribeRequest>(1);

    let _jh = create_geyser_autoconnection_task_with_mpsc(
        config.clone(),
        filter_transactions_processed(),
        autoconnect_tx.clone(),
        exit_rx.resubscribe(),
    );

    let _jh = create_geyser_autoconnection_task_with_mpsc(
        config.clone(),
        filter_transactions_finalized(),
        autoconnect_tx.clone(),
        exit_rx.resubscribe(),
    );

    let current_processed_slot = AtomicSlot::default();
    start_tracking_account_consumer(geyser_messages_rx, current_processed_slot.clone());

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}

// note: this keeps track of lot of data and might blow up memory
fn start_tracking_account_consumer(
    mut geyser_messages_rx: Receiver<Message>,
    _current_processed_slot: Arc<AtomicU64>,
) {
    tokio::spawn(async move {

        let mut processed_txs_per_block: HashMap<Slot, u64> = HashMap::new();
        let mut finalized_txs_per_block: HashMap<Slot, u64> = HashMap::new();

        'stream_loop: loop {
            match geyser_messages_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => {
                    match update.update_oneof {
                        Some(UpdateOneof::Transaction(update_tx)) => {
                            let filters = update.filters.iter().map(|s| s.to_ascii_lowercase()).collect_vec();
                            let is_processed = filters.contains(&"transaction_sub_processed".to_string());
                            let is_finalized = filters.contains(&"transaction_sub_finalized".to_string());
                            let slot = update_tx.slot;
                            let tx_info = update_tx.transaction.unwrap();
                            let tx_sig = bs58::encode(tx_info.signature).into_string();

                            if is_processed {
                                processed_txs_per_block.entry(slot).and_modify(|c| *c += 1).or_insert(1);

                            }
                            if is_finalized {
                            finalized_txs_per_block.entry(slot).and_modify(|c| *c += 1).or_insert(1);
                            }

                        }
                        Some(UpdateOneof::Slot(update)) => {
                            let slot_status = map_slot_status(&update);
                            if slot_status == CommitmentLevel::Finalized {

                                if let Some(count) = processed_txs_per_block.get(&update.slot) {
                                    info!("Processed slot {} with {} txs", update.slot, count);
                                }

                                if let Some(count) = finalized_txs_per_block.get(&update.slot) {
                                    info!("Finalized slot {} with {} txs", update.slot, count);
                                }


                            }


                        }
                        None => {}
                        _ => {}
                    }
                },
                None => {
                    log::warn!("multiplexer channel closed - aborting");
                    return;
                }
                Some(Message::Connecting(_)) => {}
            }
        }
    });
}


fn filter_transactions_processed() -> SubscribeRequest {

    let mut slot_subs = HashMap::new();
    slot_subs.insert(
        "slot_sub".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: None,
        });

    let mut transactions_subs = HashMap::new();

    transactions_subs.insert(
        "transaction_sub_processed".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );


    SubscribeRequest {
        slots: slot_subs,
        transactions: transactions_subs,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    }
}


fn filter_transactions_finalized() -> SubscribeRequest {

    let mut slot_subs = HashMap::new();
    slot_subs.insert(
        "slot_sub".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: None,
        });

    let mut transactions_subs = HashMap::new();

    transactions_subs.insert(
        "transaction_sub_finalized".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );


    SubscribeRequest {
        slots: slot_subs,
        transactions: transactions_subs,
        commitment: Some(CommitmentLevel::Finalized as i32),
        ..Default::default()
    }
}


fn map_slot_status(
    slot_update: &SubscribeUpdateSlot,
) -> solana_sdk::commitment_config::CommitmentLevel {
    use solana_sdk::commitment_config::CommitmentLevel as solanaCL;
    use yellowstone_grpc_proto::geyser::CommitmentLevel as yCL;
    yellowstone_grpc_proto::geyser::CommitmentLevel::try_from(slot_update.status)
        .map(|v| match v {
            yCL::Processed => solanaCL::Processed,
            yCL::Confirmed => solanaCL::Confirmed,
            yCL::Finalized => solanaCL::Finalized,
        })
        .expect("valid commitment level")
}

