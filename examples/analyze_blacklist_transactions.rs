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
use solana_sdk::bs58;
use tokio::sync::mpsc::{Receiver, Sender};

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use tokio::time::{sleep, Duration};
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions};

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
        all_transactions(),
        autoconnect_tx.clone(),
        exit_rx.resubscribe(),
    );

    let _jh = create_geyser_autoconnection_task_with_mpsc(
        config.clone(),
        blacklisted_transactions(),
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

        let mut tx_count_total: u64 = 0;
        let mut tx_count_blacklisted: u64 = 0;

        let mut last_prune: Slot = 0;
        let mut last_print: Slot = 0;

        'stream_loop: loop {
            match geyser_messages_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => {
                    match update.update_oneof {
                        Some(UpdateOneof::Transaction(update_tx)) => {
                            assert_eq!(update.filters.len(), 1);
                            let filter = update.filters[0].to_ascii_lowercase();
                            let is_blacklist = filter == "transaction_sub_blacklisted";
                            let slot = update_tx.slot;
                            let tx_sig = bs58::encode(update_tx.transaction.unwrap().signature).into_string();

                            // blacklisted txs appear twice

                            // info!("sig {} - bl {}", tx_sig, is_blacklist);

                            tx_count_total += 1;
                            if is_blacklist {
                                tx_count_blacklisted += 1;
                            }

                            // dump
                            if slot > last_print + 10 {
                                last_print = slot;

                                info!("slot: {}, total txs: {}, blacklisted txs: {}, % blacklisted: {:.0}%",
                                    slot, tx_count_total, tx_count_blacklisted, (tx_count_blacklisted as f64 / tx_count_total as f64) * 100.0);
                            }
                        }
                        None => {}
                        _ => {}
                    }
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


fn all_transactions() -> SubscribeRequest {
    let mut transactions_subs = HashMap::new();
    transactions_subs.insert(
        "transaction_all".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeRequest {
        transactions: transactions_subs,
        ..Default::default()
    }
}


fn blacklisted_transactions() -> SubscribeRequest {

    let account_blacklist = vec![
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

    let mut transactions_subs = HashMap::new();
    transactions_subs.insert(
        "transaction_sub_blacklisted".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: account_blacklist.iter().map(|s| s.to_string()).collect(),
            account_required: vec![],
        },
    );

    SubscribeRequest {
        transactions: transactions_subs,
        ..Default::default()
    }
}