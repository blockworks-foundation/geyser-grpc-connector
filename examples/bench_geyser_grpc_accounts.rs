use std::collections::{HashMap, VecDeque};
use futures::{Stream, StreamExt};
use log::{debug, info};
use solana_sdk::clock::{Slot, UnixTimestamp};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::env;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use itertools::Itertools;
use solana_account_decoder::parse_token::spl_token_ids;
use tokio::sync::mpsc::Receiver;

use geyser_grpc_connector::grpc_subscription_autoreconnect_streams::create_geyser_reconnecting_stream;
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::grpcmultiplex_fastestwins::{create_multiplexed_stream, FromYellowstoneExtractor};
use geyser_grpc_connector::{AtomicSlot, GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, histogram_percentiles, Message};
use tokio::time::{sleep, Duration};
use tracing::{trace, warn};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots, SubscribeUpdate};
use yellowstone_grpc_proto::prost::Message as _;

mod debouncer;

#[tokio::main]
pub async fn main() {
    // RUST_LOG=info,stream_blocks_mainnet=debug,geyser_grpc_connector=trace
    tracing_subscriber::fmt::init();
    // console_subscriber::init();

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source on {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(25),
        request_timeout: Duration::from_secs(25),
        subscribe_timeout: Duration::from_secs(25),
        receive_timeout: Duration::from_secs(25),
    };

    let config = GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, None, timeouts.clone());

    info!("Write Block stream..");

    let (autoconnect_tx, geyser_messages_rx) = tokio::sync::mpsc::channel(10);
    let (_exit, exit_notify) = tokio::sync::broadcast::channel(1);

    // let _accounts_task = create_geyser_autoconnection_task_with_mpsc(
    //     config.clone(),
    //     GeyserFilter(CommitmentConfig::processed()).accounts(),
    //     autoconnect_tx.clone(),
    //     exit_notify.resubscribe(),
    // );
    //
    // let _blocksmeta_task = create_geyser_autoconnection_task_with_mpsc(
    //     config.clone(),
    //     GeyserFilter(CommitmentConfig::processed()).blocks_meta(),
    //     autoconnect_tx.clone(),
    //     exit_notify.resubscribe(),
    // );

    let _all_accounts_and_blocksmeta_task = create_geyser_autoconnection_task_with_mpsc(
        config.clone(),
        all_accounts_and_blocksmeta(),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    // let _token_accounts_task = create_geyser_autoconnection_task_with_mpsc(
    //     config.clone(),
    //     token_accounts(),
    //     autoconnect_tx.clone(),
    //     exit_notify.resubscribe(),
    // );

    let current_processed_slot = AtomicSlot::default();
    start_tracking_slots(current_processed_slot.clone());
    start_tracking_account_consumer(geyser_messages_rx, current_processed_slot.clone());

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}



// note processed might return a slot that night end up on a fork
fn start_tracking_slots(current_processed_slot: AtomicSlot) {

    let grpc_slot_source1 = env::var("GRPC_SLOT1_ADDR").expect("need grpc url for green");
    let grpc_slot_source2 = env::var("GRPC_SLOT2_ADDR").expect("need grpc url for green");

    info!("Using grpc sources for slot: {}, {}",
        grpc_slot_source1, grpc_slot_source2
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    let config1 = GrpcSourceConfig::new(grpc_slot_source1, None, None, timeouts.clone());
    let config2 = GrpcSourceConfig::new(grpc_slot_source2, None, None, timeouts.clone());


    tokio::spawn(async move {
        debug!("start tracking slots..");

        let (multiplex_tx, mut multiplex_rx) = tokio::sync::mpsc::channel(10);
        // TODO expose
        let (_exit, exit_notify) = tokio::sync::broadcast::channel(1);

        let _blocksmeta_task1 = create_geyser_autoconnection_task_with_mpsc(
            config1.clone(),
            GeyserFilter(CommitmentConfig::processed()).slots(),
            multiplex_tx.clone(),
            exit_notify.resubscribe(),
        );

        let _blocksmeta_task2 = create_geyser_autoconnection_task_with_mpsc(
            config2.clone(),
            GeyserFilter(CommitmentConfig::processed()).slots(),
            multiplex_tx.clone(),
            exit_notify.resubscribe(),
        );

        let mut tip: Slot = 0;

        loop {
            match multiplex_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Slot(update)) => {
                        let slot = update.slot;
                        if slot > tip {
                            tip = slot;
                            current_processed_slot.store(slot, Ordering::Relaxed);
                        }
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
fn start_tracking_account_consumer(mut geyser_messages_rx: Receiver<Message>, current_processed_slot: Arc<AtomicU64>) {
    tokio::spawn(async move {

        let mut bytes_per_slot = HashMap::<Slot, usize>::new();
        let mut updates_per_slot = HashMap::<Slot, usize>::new();
        let mut wallclock_updates_per_slot_account = HashMap::<(Slot, Pubkey), Vec<SystemTime>>::new();
        // slot written by account update
        let mut current_slot: Slot = 0;

        // seconds since epoch
        let mut block_time_per_slot = HashMap::<Slot, UnixTimestamp>::new();
        // wall clock time of block completion (i.e. processed) reported by the block meta stream
        let mut block_completion_notification_time_per_slot = HashMap::<Slot, SystemTime>::new();

        let debouncer = debouncer::Debouncer::new(Duration::from_millis(5));

        loop {
            match geyser_messages_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Account(update)) => {
                        let now = SystemTime::now();
                        let account_info = update.account.unwrap();
                        let account_pk = Pubkey::try_from(account_info.pubkey).unwrap();
                        // note: slot is referencing the block that is just built while the slot number reported from BlockMeta/Slot uses the slot after the block is built
                        let slot = update.slot;
                        let account_receive_time = get_epoch_sec();

                        let latest_slot = current_processed_slot.load(Ordering::Relaxed);

                        if latest_slot != 0 {
                            // the perfect is value "-1"
                            let delta = (latest_slot as i64) - (slot as i64);
                            if debouncer.can_fire() {
                                debug!("Account info for upcoming slot {} was {} behind current processed slot", slot, delta);
                            }
                        }

                        // if account_info.data.len() > 1000 {
                        //     trace!("got account update!!! {} - {:?} - {} bytes",
                        //         slot, account_pk, account_info.data.len());
                        // }

                        bytes_per_slot.entry(slot)
                            .and_modify(|entry| *entry += account_info.data.len())
                            .or_insert(account_info.data.len());
                        updates_per_slot.entry(slot)
                            .and_modify(|entry| *entry += 1)
                            .or_insert(1);
                        wallclock_updates_per_slot_account.entry((slot, account_pk))
                            .and_modify(|entry| entry.push(now))
                            .or_insert(vec![now]);

                        if current_slot != slot {
                            info!("Slot: {}", slot);
                            if current_slot != 0 {
                                info!("Slot: {} - account data transferred: {:.2} MiB", slot, *bytes_per_slot.get(&current_slot).unwrap() as f64 / 1024.0 / 1024.0 );

                                info!("Slot: {} - num of update messages: {}", slot, updates_per_slot.get(&current_slot).unwrap());

                                let counters = wallclock_updates_per_slot_account.iter()
                                    .filter(|((slot, _pubkey), _)| slot == &current_slot)
                                    .map(|((_slot, _pubkey), updates)| updates.len() as f64)
                                    .sorted_by(|a, b| a.partial_cmp(b).unwrap())
                                    .collect_vec();
                                let count_histogram = histogram_percentiles::calculate_percentiles(&counters);
                                info!("Count histogram: {}", count_histogram);

                                if let Some(actual_block_time) = block_time_per_slot.get(&current_slot) {
                                    info!("Block time for slot {}: delta {} seconds", current_slot, account_receive_time - *actual_block_time);
                                }

                                let wallclock_minmax = wallclock_updates_per_slot_account.iter()
                                    .filter(|((slot, _pubkey), _)| slot == &current_slot)
                                    .flat_map(|((_slot, _pubkey), updates)| updates)
                                    .minmax();
                                if let Some((min, max)) = wallclock_minmax.into_option() {
                                    info!("Wallclock timestamp between first and last account update received for slot {}: {:.2}s",
                                        current_slot,
                                        max.duration_since(*min).unwrap().as_secs_f64()
                                    );
                                }


                            }
                            current_slot = slot;
                        }

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
            owner:
            spl_token_ids().iter().map(|pubkey| pubkey.to_string()).collect(),
            filters: vec![],
        },
    );


    let mut slots_subs = HashMap::new();
    slots_subs.insert("client".to_string(), SubscribeRequestFilterSlots {
        filter_by_commitment: Some(true),
    });

    let mut blocks_meta_subs = HashMap::new();
    blocks_meta_subs.insert("client".to_string(), SubscribeRequestFilterBlocksMeta {});

    SubscribeRequest {
        slots: slots_subs,
        accounts: accounts_subs,
        blocks_meta: blocks_meta_subs,
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
    slots_subs.insert("client".to_string(), SubscribeRequestFilterSlots {
        filter_by_commitment: Some(true),
    });

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




