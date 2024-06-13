use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use futures::{Stream, StreamExt};
use log::{debug, info};
use solana_sdk::clock::{Slot, UnixTimestamp};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::{env, iter};
use std::pin::pin;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use itertools::Itertools;
use solana_account_decoder::parse_token::spl_token_ids;
use solana_sdk::hash::{Hash, hash};
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

    let _all_accounts = create_geyser_autoconnection_task_with_mpsc(
        config.clone(),
        all_accounts(),
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

    let grpc_slot_source1 = env::var("GRPC_SLOT1_ADDR").expect("need grpc url for slot source1");
    let grpc_x_token_source1 = env::var("GRPC_SLOT1_X_TOKEN").ok();

    let grpc_slot_source2 = env::var("GRPC_SLOT2_ADDR").expect("need grpc url for slot source2");
    let grpc_x_token_source2 = env::var("GRPC_SLOT2_X_TOKEN").ok();

    info!("Using grpc sources for slot: {}, {}",
        grpc_slot_source1, grpc_slot_source2
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    let config1 = GrpcSourceConfig::new(grpc_slot_source1, grpc_x_token_source1, None, timeouts.clone());
    let config2 = GrpcSourceConfig::new(grpc_slot_source2, grpc_x_token_source2, None, timeouts.clone());


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
        let mut account_hashes = HashMap::<Pubkey, Vec<Hash>>::new();

        // seconds since epoch
        let mut block_time_per_slot = HashMap::<Slot, UnixTimestamp>::new();
        // wall clock time of block completion (i.e. processed) reported by the block meta stream
        let mut block_completion_notification_time_per_slot = HashMap::<Slot, SystemTime>::new();

        let debouncer = debouncer::Debouncer::new(Duration::from_millis(50));

        // Phoenix 4DoNfFBfF7UokCC2FQzriy7yHK6DY6NVdYpuekQ5pRgg
        // CzK26LWpoU9UjSrZkVu97oZj63abJrNv1zp9Hy2zZdy5
        // 6ojSigXF7nDPyhFRgmn3V9ywhYseKF9J32ZrranMGVSX
        // FV8EEHjJvDUD8Kkp1DcomTatZBA81Z6C5AhmvyUwvEAh
        // choose an account for which the diff should be calculated
        let selected_account_pk = Pubkey::from_str("4DoNfFBfF7UokCC2FQzriy7yHK6DY6NVdYpuekQ5pRgg").unwrap();

        let mut last_account_data: Option<Vec<u8>> = None;


        loop {
            match geyser_messages_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Account(update)) => {
                        let started_at = Instant::now();
                        let now = SystemTime::now();
                        let account_info = update.account.unwrap();
                        let account_pk = Pubkey::try_from(account_info.pubkey).unwrap();
                        let account_owner_pk = Pubkey::try_from(account_info.owner).unwrap();
                        // note: slot is referencing the block that is just built while the slot number reported from BlockMeta/Slot uses the slot after the block is built
                        let slot = update.slot;
                        let account_receive_time = get_epoch_sec();

                        if account_info.data.len() > 100000 {
                            let hash = hash(&account_info.data);
                            // info!("got account update!!! {} - {:?} - {} bytes - {} - {}lamps",
                            //     slot, account_pk, account_info.data.len(), hash, account_info.lamports);

                            account_hashes.entry(account_pk)
                                .and_modify(|entry| entry.push(hash))
                                .or_insert(vec![hash]);
                        }

                        // if account_hashes.len() > 100 {
                        //     for (pubkey, hashes) in &account_hashes {
                        //         info!("account hashes for {:?}", pubkey);
                        //         for hash in hashes {
                        //             info!("- hash: {}", hash);
                        //         }
                        //     }
                        // }

                        if account_pk == selected_account_pk {
                            info!("got account update!!! {} - {:?} - {} bytes - {}",
                                slot, account_pk, account_info.data.len(), account_info.lamports);

                            if let Some(prev_data) = last_account_data {
                                let hash1 = hash(&prev_data);
                                let hash2 = hash(&account_info.data);
                                info!("diff: {} {}", hash1, hash2);
                                
                                delta_compress(&prev_data, &account_info.data);

                            }

                            last_account_data = Some(account_info.data.clone());
                        }

                        bytes_per_slot.entry(slot)
                            .and_modify(|entry| *entry += account_info.data.len())
                            .or_insert(account_info.data.len());
                        updates_per_slot.entry(slot)
                            .and_modify(|entry| *entry += 1)
                            .or_insert(1);
                        wallclock_updates_per_slot_account.entry((slot, account_pk))
                            .and_modify(|entry| entry.push(now))
                            .or_insert(vec![now]);

                        if current_slot != slot && current_slot != 0 {
                            info!("New Slot: {}", slot);
                            info!("Slot: {} - account data transferred: {:.2} MiB", slot, *bytes_per_slot.get(&current_slot).unwrap() as f64 / 1024.0 / 1024.0 );

                            info!("Slot: {} - num of update messages: {}", slot, updates_per_slot.get(&current_slot).unwrap());

                            let per_account_updates = wallclock_updates_per_slot_account.iter()
                                .filter(|((slot, _pubkey), _)| slot == &current_slot)
                                .map(|((_slot, _pubkey), updates)| updates.len() as f64)
                                .sorted_by(|a, b| a.partial_cmp(b).unwrap())
                                .collect_vec();
                            let per_account_updates_histogram = histogram_percentiles::calculate_percentiles(&per_account_updates);
                            info!("Per-account updates histogram: {}", per_account_updates_histogram);

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

                        } // -- slot changed
                        current_slot = slot;


                        let latest_slot = current_processed_slot.load(Ordering::Relaxed);

                        if latest_slot != 0 {
                            // the perfect is value "-1"
                            let delta = (latest_slot as i64) - (slot as i64);
                            if debouncer.can_fire() {
                                let is_lagging = delta > -1;
                                let is_lagging_a_lot = delta - 20 > -1;
                                let info_text = if is_lagging {
                                    if is_lagging_a_lot {
                                        "A LOT"
                                    } else {
                                        "a bit"
                                    }
                                } else {
                                    "good"
                                };
                                // Account info for upcoming slot {} was {} behind current processed slot
                                debug!("Update slot {}, delta: {} - {}", slot, delta, info_text);
                            }
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

fn delta_compress(prev_data: &Vec<u8>, data: &Vec<u8>) {

    let xor_region = min(prev_data.len(), data.len());
    let mut xor_diff = vec![0u8; xor_region];

    let mut equal = 0;
    for i in 0..xor_region {
        xor_diff[i] = prev_data[i] ^ data[i];
        equal |= xor_diff[i];
    }

    if equal == 0 && prev_data.len() == data.len() {
        info!("no difference in data");
        return;
    }


    let count_non_zero = xor_diff.iter().filter(|&x| *x != 0).count();
    info!("count_non_zero={} xor_region={}", count_non_zero, xor_region);
    // info!("hex {:02X?}", xor_data);

    let compressed_xor = lz4_flex::compress_prepend_size(&xor_diff);
    info!("compressed size of xor: {} (was {})", compressed_xor.len(), xor_diff.len());

    let compressed_data = lz4_flex::compress_prepend_size(&data);
    info!("compressed size of data: {} (was {})", compressed_data.len(), data.len());

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




