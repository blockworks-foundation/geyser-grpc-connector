//
// ```
// ssh eclipse-rpc -Nv
// ```
//

use log::{info, trace};
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

        let mut total_bytes_received: u64 = 0;
        let mut blacklisted_bytes: u64 = 0;

        let mut size_per_pubkey: HashMap<Pubkey, u64> = HashMap::new();
        // use with size_per_pubkey
        let mut owner_by_pubkey: HashMap<Pubkey, Pubkey> = HashMap::new();

        let mut size_per_owner: HashMap<Pubkey, u64> = HashMap::new();

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

                        total_bytes_received += account_info.data.len() as u64;


                        // TODO
                        if is_blacklisted( &account_pk_str, &account_owner_pk_str) {
                            blacklisted_bytes += account_info.data.len() as u64;
                        }

                        // if exclude_from_analysis(&account_pk_str, &account_owner_pk_str) {
                            // info!(
                            //     "WHITELIST Account update: slot: {}, account_pk: {}, account_owner_pk: {}, data.len: {}",
                            //     slot, account_pk, account_owner_pk, account_info.data.len()
                            // );
                            // continue 'stream_loop;
                        // }

                        if slot > last_prune + 100 {
                            last_prune = slot;
                            // prune small accounts
                            size_per_pubkey.retain(|_k, v| *v > 10_000);
                            size_per_owner.retain(|_k, v| *v > 500_000);
                        }

                        // info!("Account update: slot: {}, account_pk: {}, account_owner_pk: {}, data.len: {}",
                        //     slot, account_pk, account_owner_pk, account_info.data.len()
                        // );

                        if  account_info.data.len() > 500_000 {
                            info!("LARGE Account update: slot: {}, account_pk: {}, account_owner_pk: {}, data.len: {}",
                                slot, account_pk, account_owner_pk, account_info.data.len()
                            );
                        }

                        {
                            size_per_pubkey.entry(account_pk)
                                .and_modify(|e| *e += account_info.data.len() as u64)
                                .or_insert(account_info.data.len() as u64);
                            owner_by_pubkey.entry(account_pk).or_insert(account_owner_pk);
                        }

                        {
                            size_per_owner.entry(account_owner_pk)
                                .and_modify(|e| *e += account_info.data.len() as u64)
                                .or_insert(account_info.data.len() as u64);
                        }

                        // dump
                        if slot > last_print + 10 {
                            last_print = slot;

                            if total_bytes_received > 0 {
                                info!("--- slot: {}, {:.0}%,  total bytes received: {}, blacklisted bytes: {}",
                                    slot,
                                    (100 * blacklisted_bytes) / total_bytes_received,
                                    total_bytes_received, blacklisted_bytes
                                );
                            }

                            info!("Top accounts by size (total {}):", size_per_pubkey.len());
                            let dump_started_at = Instant::now();
                            for (pk, size) in size_per_pubkey.iter().sorted_by(|a, b| b.1.cmp(a.1)).take(10) {
                                let owner = owner_by_pubkey.get(pk).unwrap();
                                info!("  {} (owner {}): {}", pk, owner, size);
                            };

                            info!("Top owners by size (total {}):", size_per_owner.len());
                            for (owner, size) in size_per_owner.iter().sorted_by(|a, b| b.1.cmp(a.1)).take(10) {
                                info!("  {}: {}", owner, size);
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

// proposed blacklist
fn is_blacklisted(pubkey: &str, owner: &str) -> bool {
    // - token accounts
    //     - srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX   (openbook)
    //     - opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb (openbook v2)
    //     - Stake11111111111111111111111111111111111111
    //     - 4DoNfFBfF7UokCC2FQzriy7yHK6DY6NVdYpuekQ5pRgg Phoenix (WSOL-USDC) Market, 1.7 MB
    //     - Ew3vFDdtdGrknJAVVfraxCA37uNJtimXYPY4QjnfhFHH Phoenix (WETH-USDC) Market, 1.7 MB
    //     - PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY
    //     - LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo  Meteora DLMM Program
    //     - 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin Serum DEX V3
    let pubkey_blacklist = vec![
    ];
    let owner_blacklist = vec![
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
        // OpenBook
        "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",
        // OpenBook V2
        "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb",
        "Stake11111111111111111111111111111111111111",
        // Phoenix (Phoenix Markets like WETH-USDC)
        "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY",
        // Meteora DLMM Program
        "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
        // Serum DEX V3
        "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
        // Metaplex Token Metadata
        "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
        // Jito Tip Distribution
        "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7",
        // Kamino
        "HFn8GnPADiny6XqUoWE8uRPPxb29ikn4yTuPa9MF2fWJ",
        // BPF Loader
        "BPFLoader2111111111111111111111111111111111",
        "BPFLoaderUpgradeab1e11111111111111111111111",
        // Vote program
        "Vote111111111111111111111111111111111111111",
        // Raydium Concentrated Liquidity
        "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
    ];

    let blacklisted_by_pubkey = pubkey_blacklist.contains(&pubkey);
    let blacklisted_by_owner = owner_blacklist.contains(&owner);

    if owner_blacklist.contains(&pubkey) && owner != "BPFLoader2111111111111111111111111111111111" && owner != "BPFLoaderUpgradeab1e11111111111111111111111" {
        // this is an error in the blacklist definition
        panic!("BLACKLIST key is NOT a pubkey - Account: {}, owner: {}", pubkey, owner);
    }

    if pubkey_blacklist.contains(&owner) {
        // this is an error in the blacklist definition
        panic!("BLACKLIST pubkey is NOT an owner - Account: {}, owner: {}", pubkey, owner);
    }

    if blacklisted_by_pubkey {
        trace!("BLACKLISTED by pubkey - Account: {}, owner: {}", pubkey, owner);
        return true;
    }

    if blacklisted_by_owner {
        trace!("BLACKLISTED by owner - Account: {}, owner: {}", pubkey, owner);
        return true;
    }

    return false;
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

