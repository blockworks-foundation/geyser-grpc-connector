use std::collections::HashMap;
use futures::{Stream, StreamExt};
use log::{debug, info, trace};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::env;
use std::pin::pin;
use std::str::FromStr;
use std::sync::Arc;
use dashmap::DashMap;
use solana_account_decoder::parse_token::{parse_token, spl_token_ids, TokenAccountType, UiTokenAccount};
use solana_account_decoder::parse_token::UiAccountState::Initialized;
use solana_sdk::program_utils::limited_deserialize;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::vote::instruction::VoteInstruction;
use solana_sdk::vote::state::Vote;

use geyser_grpc_connector::grpc_subscription_autoreconnect_streams::create_geyser_reconnecting_stream;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use tokio::time::{sleep, Duration};
use tracing::field::debug;
use tracing::warn;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdate};
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

    let green_stream = create_geyser_reconnecting_stream(
        config.clone(),
        transaction_filter(),
    );

    tokio::spawn(async move {
        let mut green_stream = pin!(green_stream);
        while let Some(message) = green_stream.next().await {
            match message {
                Message::GeyserSubscribeUpdate(subscriber_update) => {
                    match subscriber_update.update_oneof {
                        Some(UpdateOneof::Transaction(update)) => {

                            let message = update.transaction.unwrap().transaction.unwrap().message.unwrap();


                            // https://docs.solanalabs.com/implemented-proposals/validator-timestamp-oracle
                            for ci in message.instructions {
                                let vote_instruction = limited_deserialize::<VoteInstruction>(&ci.data).unwrap();
                                info!("vote_instruction: {:?}", vote_instruction);
                                info!("vote_instruction: {:?}", vote_instruction.timestamp().unwrap());
                            }

                            // let is_vote_transaction = message.instructions().iter().any(|i| {
                            //     i.program_id(message.static_account_keys())
                            //         .eq(&solana_sdk::vote::program::id())
                            //         && limited_deserialize::<VoteInstruction>(&i.data)
                            //         .map(|vi| vi.is_simple_vote())
                            //         .unwrap_or(false)
                            // });



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
    }
}

