use dashmap::DashMap;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use log::{debug, info, trace};
use solana_account_decoder::parse_token::UiAccountState::Initialized;
use solana_account_decoder::parse_token::{
    parse_token, spl_token_ids, TokenAccountType, UiTokenAccount,
};
use solana_sdk::clock::{Slot, UnixTimestamp};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::program_utils::limited_deserialize;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::vote::instruction::VoteInstruction;
use solana_sdk::vote::state::Vote;
use std::collections::{HashMap, HashSet};
use std::env;
use std::pin::pin;
use std::str::FromStr;
use std::sync::Arc;

use geyser_grpc_connector::grpc_subscription_autoreconnect_streams::create_geyser_reconnecting_stream;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::histogram_percentiles::calculate_percentiles;
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use tokio::time::{sleep, Duration};
use tracing::field::debug;
use tracing::warn;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeUpdate,
};
use yellowstone_grpc_proto::prost::Message as _;

const ENABLE_TIMESTAMP_TAGGING: bool = false;

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

    let green_stream = create_geyser_reconnecting_stream(config.clone(), transaction_filter());

    tokio::spawn(async move {
        let mut vote_times_by_slot: HashMap<Slot, HashSet<UnixTimestamp>> = HashMap::new();

        let mut green_stream = pin!(green_stream);
        while let Some(message) = green_stream.next().await {
            match message {
                Message::GeyserSubscribeUpdate(subscriber_update) => {
                    match subscriber_update.update_oneof {
                        Some(UpdateOneof::Transaction(update)) => {
                            let message = update
                                .transaction
                                .unwrap()
                                .transaction
                                .unwrap()
                                .message
                                .unwrap();
                            let slot = update.slot;

                            // https://docs.solanalabs.com/implemented-proposals/validator-timestamp-oracle
                            for ci in message.instructions {
                                let vote_instruction =
                                    limited_deserialize::<VoteInstruction>(&ci.data).unwrap();
                                let last_voted_slot = vote_instruction.last_voted_slot().unwrap();
                                info!(
                                    "vote_instruction: {:?}",
                                    vote_instruction.timestamp().unwrap()
                                );
                                vote_times_by_slot
                                    .entry(last_voted_slot)
                                    .or_insert(HashSet::new())
                                    .insert(vote_instruction.timestamp().unwrap());
                            }

                            // hack to look at reasonable settled slot
                            // print_spread(&vote_times_by_slot, slot);
                            if vote_times_by_slot.contains_key(&(slot - 10)) {
                                print_spread(&vote_times_by_slot, slot - 10);
                            }
                        }
                        _ => {}
                    }
                }
                Message::Connecting(attempt) => {
                    warn!("Connection attempt: {}", attempt);
                }
            }
        }
        warn!("Stream aborted");
    });

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}

fn print_spread(vote_times_by_slot: &HashMap<Slot, HashSet<UnixTimestamp>>, slot: Slot) {
    let slots = vote_times_by_slot.get(&slot).unwrap();
    let min_slot = slots.iter().min().unwrap();
    let array = slots
        .iter()
        .sorted()
        .map(|x| (*x - min_slot) as f64)
        .collect_vec();
    let histo = calculate_percentiles(&array);
    info!("slot: {} histo: {}", slot, histo);
}

pub fn transaction_filter() -> SubscribeRequest {
    let mut trnasactions_subs = HashMap::new();
    trnasactions_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(true),
            failed: Some(false),
            signature: None,
            // TODO
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeRequest {
        slots: HashMap::new(),
        accounts: HashMap::new(),
        transactions: trnasactions_subs,
        entry: Default::default(),
        blocks: Default::default(),
        blocks_meta: HashMap::new(),
        commitment: None,
        accounts_data_slice: Default::default(),
        ping: None,
        transactions_status: Default::default(),
    }
}
