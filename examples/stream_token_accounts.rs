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
use solana_sdk::pubkey::Pubkey;

use geyser_grpc_connector::grpc_subscription_autoreconnect_streams::create_geyser_reconnecting_stream;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use tokio::time::{sleep, Duration};
use tracing::field::debug;
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

    // owner x mint -> amount
    let token_account_by_ownermint: Arc<DashMap<Pubkey, DashMap<Pubkey, UiTokenAccount>>> = Arc::new(DashMap::with_capacity(10000));
    let token_account_by_ownermint_read = token_account_by_ownermint.clone();
    let token_account_by_ownermint = token_account_by_ownermint.clone();


    tokio::spawn(async move {
        let mut green_stream = pin!(green_stream);
        while let Some(message) = green_stream.next().await {
            match message {
                Message::GeyserSubscribeUpdate(subscriber_update) => {
                    match subscriber_update.update_oneof {
                        Some(UpdateOneof::Account(update)) => {
                            let account = update.account.unwrap();
                            let account_pk = Pubkey::try_from(account.pubkey).unwrap();
                            trace!("got account update (green)!!! {} - {:?} - {} bytes",
                                update.slot, account_pk, account.data.len());

                            match parse_token(&account.data, Some(6)) {
                                Ok(TokenAccountType::Account(account_ui)) => {
                                    // UiTokenAccount {
                                    //   mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                                    //   owner: "9un5wqE3q4oCjyrDkwsdD48KteCJitQX5978Vh7KKxHo",
                                    //   token_amount: UiTokenAmount {
                                    //     ui_amount: Some(8229995.070667),
                                    //     decimals: 6, amount: "8229995070667",
                                    //     ui_amount_string: "8229995.070667"
                                    //   },
                                    //   delegate: None,
                                    //   state: Initialized,
                                    //   is_native: false,
                                    //   rent_exempt_reserve: None,
                                    //   delegated_amount: None,
                                    //   close_authority: None,
                                    //   extensions: []
                                    // }
                                    if matches!(account_ui.state, Initialized) {
                                        let owner = Pubkey::from_str(&account_ui.owner).unwrap();
                                        let mint = Pubkey::from_str(&account_ui.mint).unwrap();
                                        // 6 decimals as requested
                                        let amount = &account_ui.token_amount.amount;
                                        // groovie wallet
                                        if account_ui.owner.starts_with("66fEFnKy") {
                                            info!("update balance for mint {} of owner {}: {}", mint, owner, amount);
                                        }
                                        token_account_by_ownermint.entry(owner)
                                            .or_insert_with(DashMap::new)
                                            .insert(mint, account_ui);
                                    }
                                }
                                Ok(TokenAccountType::Mint(mint)) => {
                                    // not interesting
                                }
                                Ok(TokenAccountType::Multisig(_)) => {}
                                Err(parse_error) => {
                                    trace!("Could not parse account {} - {}", account_pk, parse_error);
                                }
                            }


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


    tokio::spawn(async move {

        loop {
            let mut total = 0;
            for accounts_by_mint in token_account_by_ownermint_read.iter() {
                for token_account_mint in accounts_by_mint.iter() {
                    total += 1;
                    let (owner, mint, account) = (accounts_by_mint.key(), token_account_mint.key(), token_account_mint.value());
                    debug!("{} - {} - {}", owner, mint, account.token_amount.ui_amount_string);
                }
            }
            info!("Total owner x mint entries in cache map: {}", total);
            sleep(Duration::from_millis(50)).await;
        }

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

