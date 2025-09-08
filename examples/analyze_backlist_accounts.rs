//
// ```
// ssh eclipse-rpc -Nv
// ```
//

use log::info;
use solana_account_decoder::parse_token::spl_token_ids;
use solana_sdk::clock::{Slot, UnixTimestamp};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use itertools::Itertools;
use tokio::sync::mpsc::{Receiver, Sender};

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use tokio::time::{sleep, Duration};
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots};
use yellowstone_grpc_proto::geyser::subscribe_request_filter_accounts_filter::Filter::Memcmp;
use yellowstone_grpc_proto::geyser::subscribe_request_filter_accounts_filter_memcmp::Data::Base58;

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
        all_accounts(),
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

        let mut size_per_pubkey: HashMap<Pubkey, u64> = HashMap::new();
        let mut owner_by_pubkey: HashMap<Pubkey, Pubkey> = HashMap::new();
        let mut last_prune: Slot = 0;
        let mut last_print: Slot = 0;

        'stream_loop: loop {
            match geyser_messages_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Account(update)) => {
                        let account_info = update.account.unwrap();
                        let account_pk = Pubkey::try_from(account_info.pubkey).unwrap();
                        let account_pk_str = account_pk.to_string();
                        let account_owner_pk = Pubkey::try_from(account_info.owner).unwrap();
                        let account_owner_pk_str = account_owner_pk.to_string();
                        let slot = update.slot;

                        if exclude_from_analysis(&account_pk_str, &account_owner_pk_str) {
                            // info!(
                            //     "WHITELIST Account update: slot: {}, account_pk: {}, account_owner_pk: {}, data.len: {}",
                            //     slot, account_pk, account_owner_pk, account_info.data.len()
                            // );
                            continue 'stream_loop;
                        }

                        if slot > last_prune + 100 {
                            last_prune = slot;
                            // prune small accounts
                            size_per_pubkey.retain(|_k, v| *v > 10_000);
                        }

                        // info!("Account update: slot: {}, account_pk: {}, account_owner_pk: {}, data.len: {}",
                        //     slot, account_pk, account_owner_pk, account_info.data.len()
                        // );

                        if  account_info.data.len() > 500_000 {
                            info!("LARGE Account update: slot: {}, account_pk: {}, account_owner_pk: {}, data.len: {}",
                                slot, account_pk, account_owner_pk, account_info.data.len()
                            );
                        }

                        size_per_pubkey.entry(account_pk)
                            .and_modify(|e| *e += account_info.data.len() as u64)
                            .or_insert(account_info.data.len() as u64);
                        owner_by_pubkey.entry(account_pk).or_insert(account_owner_pk);

                        // dump
                        if slot > last_print + 10 {
                            last_print = slot;
                            info!("Top accounts by size (total {}):", size_per_pubkey.len());
                            let dump_started_at = Instant::now();
                            for (pk, size) in size_per_pubkey.iter().sorted_by(|a, b| b.1.cmp(a.1)).take(10) {
                                let owner = owner_by_pubkey.get(pk).unwrap();
                                info!("  {} (owner {}): {}", pk, owner, size);
                            };
                        }

                        // info!(
                        //     "Account update: slot: {}, account_pk: {}, account_owner_pk: {}, data.len: {}",
                        //     slot, account_pk, account_owner_pk, account_info.data.len()
                        // );
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

// don't include these well known accounts into analysis
fn exclude_from_analysis(pubkey: &str, owner: &str) -> bool {
    const RAYDIUM_AMM_PUBKEY: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

    let pubkey_lower = pubkey.to_ascii_lowercase();
    let owner_lower = owner.to_ascii_lowercase();

    if owner_lower.contains("jup") || owner_lower.contains("jito") {
        return true;
    }

    if owner == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" || owner == "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" {
        return true;
    }

    if owner == RAYDIUM_AMM_PUBKEY {
        return true;
    }

    return false;
}

fn all_accounts() -> SubscribeRequest {
    let mut accounts_subs = HashMap::new();
    accounts_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![],
            filters: vec![],
            nonempty_txn_signature: None,
        },
    );

    SubscribeRequest {
        accounts: accounts_subs,
        ..Default::default()
    }
}

