//
// ```
// ssh eclipse-rpc -Nv
// ```
//

use log::{info, trace};
use solana_account_decoder::parse_token::spl_token_ids;
use solana_sdk::clock::UnixTimestamp;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::env;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use itertools::Itertools;
use tokio::sync::mpsc::Receiver;

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use tokio::time::{sleep, Duration};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterSlots,
};

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

    let config = GrpcSourceConfig::new(grpc_addr, grpc_x_token, None, timeouts.clone());

    let (autoconnect_tx, geyser_messages_rx) = tokio::sync::mpsc::channel(10);
    let (_exit_tx, exit_rx) = tokio::sync::broadcast::channel::<()>(1);

    let _all_accounts = create_geyser_autoconnection_task_with_mpsc(
        config.clone(),
        all_accounts(),
        autoconnect_tx.clone(),
        exit_rx.resubscribe(),
    );

    let current_processed_slot = AtomicSlot::default();
    start_account_scanner_sol_changes(geyser_messages_rx, current_processed_slot.clone());

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}

// note: this keeps track of lot of data and might blow up memory
fn start_tracking_account_consumer(
    mut geyser_messages_rx: Receiver<Message>,
    _current_processed_slot: Arc<AtomicU64>,
) {
    tokio::spawn(async move {
        loop {
            match geyser_messages_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Account(update)) => {
                        let account_info = update.account.unwrap();
                        let account_pk = Pubkey::try_from(account_info.pubkey).unwrap();
                        let account_owner_pk = Pubkey::try_from(account_info.owner).unwrap();
                        // note: slot is referencing the block that is just built while the slot number reported from BlockMeta/Slot uses the slot after the block is built
                        let slot = update.slot;
                        let account_receive_time = get_epoch_sec();

                        info!(
                            "Account update: slot: {}, account_pk: {}, account_owner_pk: {}, account_receive_time: {}",
                            slot, account_pk, account_owner_pk, account_receive_time
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
    });
}


// note: this keeps track of lot of data and might blow up memory
fn start_account_scanner_sol_changes(
    mut geyser_messages_rx: Receiver<Message>,
    _current_processed_slot: Arc<AtomicU64>,
) {
    tokio::spawn(async move {

        // note: account_pk -> (count, last_lamport)
        let mut account_lamp_changes: HashMap<Pubkey, (u64, u64)> = HashMap::new();
        let mut loop_count = 0;

        loop {
            loop_count += 1;
            match geyser_messages_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Account(update)) => {
                        let account_info = update.account.unwrap();
                        let account_pk = Pubkey::try_from(account_info.pubkey).unwrap();
                        let account_owner_pk = Pubkey::try_from(account_info.owner).unwrap();
                        // note: slot is referencing the block that is just built while the slot number reported from BlockMeta/Slot uses the slot after the block is built
                        let slot = update.slot;
                        let account_receive_time = get_epoch_sec();

                        let (count, last_lamport) = account_lamp_changes.entry(account_pk).or_insert((0, 999));
                        if *last_lamport != account_info.lamports {
                            *count += 1;
                            *last_lamport = account_info.lamports;
                        }

                        if loop_count % 100_000 == 0 {
                            info!("loop count: {}", loop_count);
                            info!("accounts: {}", account_lamp_changes.len());
                            info!("account changes: {:?}", account_lamp_changes.get(&account_pk));
                            let top_100 = account_lamp_changes.iter().filter(|(_k, (count, _lamp))| *count > 100)
                                .sorted_unstable_by_key(|(_k, (count, _lamp))| u64::MAX - count)
                                .take(100)
                                .collect_vec();
                            info!("top 100: {:?}", top_100);

                        }

                        trace!(
                            "Account update: slot: {}, account_pk: {}, account_owner_pk: {}, account_receive_time: {}",
                            slot, account_pk, account_owner_pk, account_receive_time
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
    });
}


fn get_epoch_sec() -> UnixTimestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as UnixTimestamp
}

pub fn token_accounts() -> SubscribeRequest {
    let mut accounts_subs = HashMap::new();
    accounts_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            // vec!["4DoNfFBfF7UokCC2FQzriy7yHK6DY6NVdYpuekQ5pRgg".to_string()],
            owner: spl_token_ids()
                .iter()
                .map(|pubkey| pubkey.to_string())
                .collect(),
            filters: vec![],
        },
    );

    SubscribeRequest {
        accounts: accounts_subs,
        ..Default::default()
    }
}

pub fn all_accounts_and_blocksmeta() -> SubscribeRequest {
    let mut accounts_subs = HashMap::new();
    accounts_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![],
            filters: vec![],
        },
    );

    let mut slots_subs = HashMap::new();
    slots_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
        },
    );

    let mut blocks_meta_subs = HashMap::new();
    blocks_meta_subs.insert("client".to_string(), SubscribeRequestFilterBlocksMeta {});

    SubscribeRequest {
        slots: slots_subs,
        accounts: accounts_subs,
        blocks_meta: blocks_meta_subs,
        ..Default::default()
    }
}

pub fn all_accounts() -> SubscribeRequest {
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
        accounts: accounts_subs,
        ..Default::default()
    }
}


pub fn jup_account() -> SubscribeRequest {
    let mut accounts_subs = HashMap::new();
    accounts_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec!["3hvmJXPFW7ERXvWRDx6v9oKShwsxRsYn8jZoNddiHcaJ".to_string()],
            owner: vec![],
            filters: vec![],
        },
    );

    SubscribeRequest {
        accounts: accounts_subs,
        ..Default::default()
    }
}

pub fn slots() -> SubscribeRequest {
    let mut slots_subs = HashMap::new();
    slots_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
        },
    );

    SubscribeRequest {
        slots: slots_subs,
        ..Default::default()
    }
}
