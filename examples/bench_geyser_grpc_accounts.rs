use std::collections::{HashMap, VecDeque};
use futures::{Stream, StreamExt};
use log::info;
use solana_sdk::clock::{Slot, UnixTimestamp};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::env;
use std::pin::pin;
use std::time::{SystemTime, UNIX_EPOCH};
use itertools::Itertools;
use tokio::sync::mpsc::Receiver;

use geyser_grpc_connector::grpc_subscription_autoreconnect_streams::create_geyser_reconnecting_stream;
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, histogram_percentiles, Message};
use tokio::time::{sleep, Duration};
use tracing::warn;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use yellowstone_grpc_proto::prost::Message as _;

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

    let _accounts_task = create_geyser_autoconnection_task_with_mpsc(
        config.clone(),
        GeyserFilter(CommitmentConfig::processed()).accounts(),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    let _blocksmeta_task = create_geyser_autoconnection_task_with_mpsc(
        config.clone(),
        GeyserFilter(CommitmentConfig::processed()).blocks_meta(),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    start_tracking_account_consumer(geyser_messages_rx);

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}

// note: this keeps track of lot of data and might blow up memory
fn start_tracking_account_consumer(mut geyser_messages_rx: Receiver<Message>) {
    const RECENT_SLOTS_LIMIT: usize = 30;

    tokio::spawn(async move {

        let mut bytes_per_slot = HashMap::<Slot, usize>::new();
        let mut updates_per_slot = HashMap::<Slot, usize>::new();
        let mut wallclock_updates_per_slot_account = HashMap::<(Slot, Pubkey), Vec<SystemTime>>::new();
        // slot written by account update
        let mut current_slot: Slot = 0;
        // slot from slot stream
        let mut slot_just_completed: Slot = 0;

        // seconds since epoch
        let mut block_time_per_slot = HashMap::<Slot, UnixTimestamp>::new();
        // wall clock time of block completion (i.e. processed) reported by the block meta stream
        let mut block_completion_notification_time_per_slot = HashMap::<Slot, SystemTime>::new();
        let mut recent_slot_deltas: VecDeque<i64> = VecDeque::with_capacity(RECENT_SLOTS_LIMIT);

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

                        if slot_just_completed != slot {
                            if slot_just_completed != 0 {
                                // the perfect is value "-1"
                                recent_slot_deltas.push_back((slot_just_completed as i64) - (slot as i64));
                                if recent_slot_deltas.len() > RECENT_SLOTS_LIMIT {
                                    recent_slot_deltas.pop_front();
                                }
                            }
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

                        if current_slot != slot {
                            info!("Slot: {}", slot);
                            if current_slot != 0 {
                                info!("Slot: {} - {:.2} MiB", slot, *bytes_per_slot.get(&current_slot).unwrap() as f64 / 1024.0 / 1024.0 );

                                info!("Slot: {} - Updates: {}", slot, updates_per_slot.get(&current_slot).unwrap());

                                let counters = wallclock_updates_per_slot_account.iter()
                                    .filter(|((slot, _pubkey), _)| slot == &current_slot)
                                    .map(|((_slot, _pubkey), updates)| updates.len() as f64)
                                    .sorted_by(|a, b| a.partial_cmp(b).unwrap())
                                    .collect_vec();
                                let count_histogram = histogram_percentiles::calculate_percentiles(&counters);
                                info!("Count histogram: {}", count_histogram);

                                let deltas = recent_slot_deltas.iter()
                                    .map(|x| *x as f64)
                                    .sorted_by(|a, b| a.partial_cmp(b).unwrap())
                                    .collect_vec();
                                let deltas_histogram = histogram_percentiles::calculate_percentiles(&deltas);
                                info!("Deltas slots list: {:?}", recent_slot_deltas);
                                info!("Deltas histogram: {}", deltas_histogram);

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
                    Some(UpdateOneof::BlockMeta(update)) => {
                        let now = SystemTime::now();
                        // completed depends on commitment level which is processed ATM
                        slot_just_completed = update.slot;
                        block_time_per_slot.insert(slot_just_completed, update.block_time.unwrap().timestamp);
                        block_completion_notification_time_per_slot.insert(slot_just_completed, now);
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