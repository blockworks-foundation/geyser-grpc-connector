use std::collections::HashMap;
use futures::{Stream, StreamExt};
use log::info;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::env;
use std::pin::pin;
use std::str::FromStr;
use solana_account_decoder::parse_token::{parse_token, spl_token_ids, TokenAccountType};
use solana_sdk::pubkey::Pubkey;

use geyser_grpc_connector::grpc_subscription_autoreconnect_streams::create_geyser_reconnecting_stream;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use tokio::time::{sleep, Duration};
use tracing::warn;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeUpdate};
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

    let green_stream = create_geyser_reconnecting_stream(
        config.clone(),
        token_accounts(),
    );


    tokio::spawn(async move {
        let mut green_stream = pin!(green_stream);
        while let Some(message) = green_stream.next().await {
            match message {
                Message::GeyserSubscribeUpdate(subscriber_update) => {
                    match subscriber_update.update_oneof {
                        Some(UpdateOneof::Account(update)) => {
                            let account = update.account.unwrap();
                            let account_pk = Pubkey::try_from(account.pubkey).unwrap();

                            let account_type = parse_token(&account.data, Some(6)).unwrap();
                            match account_type {
                                TokenAccountType::Account(account_ui) => {
                                    // UiTokenAccount {
                                    //   mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                                    //   owner: "7XDMxfmzmL2Hqhh8ABp4Byc5UEsMWfQdWo6r5vEVQNb8",
                                    //   token_amount: UiTokenAmount {
                                    //     ui_amount: Some(0.0), decimals: 6, amount: "0", ui_amount_string: "0"
                                    //   },
                                    //   delegate: None,
                                    //   state: Initialized,
                                    //   is_native: false,
                                    //   rent_exempt_reserve: None,
                                    //   delegated_amount: None,
                                    //   close_authority: None,
                                    //   extensions: []
                                    // }
                                    info!("it's an Account: {:?}", account_ui);
                                }
                                TokenAccountType::Mint(mint) => {
                                    // not interesting
                                }
                                TokenAccountType::Multisig(_) => {}
                            }

                            info!("got account update (green)!!! {} - {:?} - {} bytes",
                                update.slot, account_pk, account.data.len());
                            let bytes: [u8; 32] =
                                account_pk.to_bytes();
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

pub fn token_accounts() -> SubscribeRequest {
    let mut accounts_subs = HashMap::new();
    accounts_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner:
                spl_token_ids().iter().map(|pubkey| pubkey.to_string()).collect(),
            filters: vec![],
        },
    );


    SubscribeRequest {
        slots: HashMap::new(),
        accounts: accounts_subs,
        transactions: HashMap::new(),
        entry: Default::default(),
        blocks: Default::default(),
        blocks_meta: HashMap::new(),
        commitment: None,
        accounts_data_slice: Default::default(),
        ping: None,
    }
}

