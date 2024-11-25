///
/// subsribe to grpc in multiple ways:
/// - all slots and processed accounts in one subscription
/// - only processed accounts
/// - only confirmed accounts
/// - only finalized accounts
///
/// we want to see if there is a difference in timing of "processed accounts" in the mix with slot vs "only processed accounts"
use log::{info, warn};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::time::SystemTime;

use base64::Engine;
use csv::ReaderBuilder;
use itertools::Itertools;

/// This file mocks the core model of the RPC server.
use solana_sdk::compute_budget;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::message::v0::MessageAddressTableLookup;
use solana_sdk::message::{v0, MessageHeader, VersionedMessage};
use solana_sdk::pubkey::Pubkey;

use solana_sdk::signature::Signature;
use solana_sdk::transaction::TransactionError;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Receiver;

use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    SubscribeUpdateSlot,
};

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::{
    map_commitment_level, GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, Message,
};
use tokio::time::{sleep, Duration};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccounts;

fn start_all_slots_and_processed_accounts_consumer(mut slots_channel: Receiver<Message>) {
    tokio::spawn(async move {
        loop {
            match slots_channel.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Slot(update_slot)) => {
                        let since_epoch_ms = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_millis();

                        let short_status = match map_slot_status(&update_slot) {
                            CommitmentLevel::Processed => "P",
                            CommitmentLevel::Confirmed => "C",
                            CommitmentLevel::Finalized => "F",
                            _ => {
                                panic!("unexpected commitment level")
                            }
                        };
                        // DUMPSLOT 283356662,283356661,F,1723556492340
                        info!(
                            "MIXSLOT {},{:09},{},{}",
                            update_slot.slot,
                            update_slot.parent.unwrap_or(0),
                            short_status,
                            since_epoch_ms
                        );
                    }
                    Some(UpdateOneof::Account(update_account)) => {
                        let since_epoch_ms = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_millis();

                        let account_info = update_account.account.unwrap();
                        let slot = update_account.slot;
                        let account_pk =
                            Pubkey::new_from_array(account_info.pubkey.try_into().unwrap());
                        let write_version = account_info.write_version;
                        let data_len = account_info.data.len();
                        // DUMPACCOUNT 283417593,HTQeo4GNbZfGY5G4fAkDr1S5xnz5qWXFgueRwgw53aU1,1332997857270,752,1723582355872
                        info!(
                            "MIXACCOUNT {},{},{},{},{}",
                            slot, account_pk, write_version, data_len, since_epoch_ms
                        );
                    }
                    None => {}
                    _ => {}
                },
                None => {
                    warn!("multiplexer channel closed - aborting");
                    return;
                }
                Some(Message::Connecting(_)) => {}
            }
        }
    });
}

// need to provide the commitment level used to filter the accounts
fn start_account_same_level(
    mut slots_channel: Receiver<Message>,
    commitment_level: CommitmentLevel,
) {
    tokio::spawn(async move {
        loop {
            match slots_channel.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Account(update_account)) => {
                        let since_epoch_ms = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_millis();

                        let account_info = update_account.account.unwrap();
                        let slot = update_account.slot;
                        let account_pk =
                            Pubkey::new_from_array(account_info.pubkey.try_into().unwrap());
                        let write_version = account_info.write_version;
                        let data_len = account_info.data.len();

                        let short_status = match commitment_level {
                            CommitmentLevel::Processed => "P",
                            CommitmentLevel::Confirmed => "C",
                            CommitmentLevel::Finalized => "F",
                            _ => {
                                panic!("unexpected commitment level")
                            }
                        };

                        // DUMPACCOUNT 283417593,HTQeo4GNbZfGY5G4fAkDr1S5xnz5qWXFgueRwgw53aU1,1332997857270,752,1723582355872
                        info!(
                            "DUMPACCOUNT {},{},{},{},{},{}",
                            slot, short_status, account_pk, write_version, data_len, since_epoch_ms
                        );
                    }
                    None => {}
                    _ => {}
                },
                None => {
                    warn!("multiplexer channel closed - aborting");
                    return;
                }
                Some(Message::Connecting(_)) => {}
            }
        }
    });
}

fn map_slot_status(
    slot_update: &SubscribeUpdateSlot,
) -> solana_sdk::commitment_config::CommitmentLevel {
    use solana_sdk::commitment_config::CommitmentLevel as solanaCL;
    use yellowstone_grpc_proto::geyser::CommitmentLevel as yCL;
    yellowstone_grpc_proto::geyser::CommitmentLevel::try_from(slot_update.status)
        .map(|v| match v {
            yCL::Processed => solanaCL::Processed,
            yCL::Confirmed => solanaCL::Confirmed,
            yCL::Finalized => solanaCL::Finalized,
        })
        .expect("valid commitment level")
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using gRPC source {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    let green_config =
        GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, None, timeouts.clone());

    let (_, exit_notify) = broadcast::channel(1);

    // mix of (all) slots and processed accounts
    let (autoconnect_tx, slots_accounts_rx) = tokio::sync::mpsc::channel(10);
    let _green_stream_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        all_slots_and_processed_accounts_together(),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    let (only_processed_accounts_tx, only_processed_accounts_rx) = tokio::sync::mpsc::channel(10);
    let _accounts_processed_stream_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        accounts_at_level(CommitmentLevel::Processed),
        only_processed_accounts_tx.clone(),
        exit_notify.resubscribe(),
    );

    let (only_confirmed_accounts_tx, only_confirmed_accounts_rx) = tokio::sync::mpsc::channel(10);
    let _accounts_confirmed_stream_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        accounts_at_level(CommitmentLevel::Confirmed),
        only_confirmed_accounts_tx.clone(),
        exit_notify.resubscribe(),
    );

    let (only_finalized_accounts_tx, only_finalized_accounts_rx) = tokio::sync::mpsc::channel(10);
    let _accounts_finalized_stream_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        accounts_at_level(CommitmentLevel::Finalized),
        only_finalized_accounts_tx.clone(),
        exit_notify.resubscribe(),
    );

    start_all_slots_and_processed_accounts_consumer(slots_accounts_rx);
    start_account_same_level(only_processed_accounts_rx, CommitmentLevel::Processed);
    start_account_same_level(only_confirmed_accounts_rx, CommitmentLevel::Confirmed);
    start_account_same_level(only_finalized_accounts_rx, CommitmentLevel::Finalized);

    // "infinite" sleep
    sleep(Duration::from_secs(3600 * 5)).await;
}

const RAYDIUM_AMM_PUBKEY: &'static str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

fn all_slots_and_processed_accounts_together() -> SubscribeRequest {
    let mut slot_subs = HashMap::new();
    slot_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterSlots {
            // implies all slots
            filter_by_commitment: None,
        },
    );
    let mut account_subs = HashMap::new();
    account_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![RAYDIUM_AMM_PUBKEY.to_string()],
            filters: vec![],
        },
    );

    SubscribeRequest {
        slots: slot_subs,
        accounts: account_subs,
        ping: None,
        // implies "processed"
        commitment: None,
        ..Default::default()
    }
}

fn accounts_at_level(commitment_level: CommitmentLevel) -> SubscribeRequest {
    let mut account_subs = HashMap::new();
    account_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![RAYDIUM_AMM_PUBKEY.to_string()],
            filters: vec![],
        },
    );

    SubscribeRequest {
        accounts: account_subs,
        ping: None,
        commitment: Some(map_commitment_level(CommitmentConfig {
            commitment: commitment_level,
        }) as i32),
        ..Default::default()
    }
}

#[test]
fn parse_output() {
    let data = "283360248,000000000,C,1723558000558";
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(data.as_bytes());

    let all_records = rdr.records().collect_vec();
    assert_eq!(1, all_records.len());
    let record = all_records[0].as_ref().unwrap();

    let slot: u64 = record[0].parse().unwrap();
    let parent: Option<u64> = record[1]
        .parse()
        .ok()
        .and_then(|v| if v == 0 { None } else { Some(v) });
    let status = match record[2].to_string().as_str() {
        "P" => CommitmentLevel::Processed,
        "C" => CommitmentLevel::Confirmed,
        "F" => CommitmentLevel::Finalized,
        _ => panic!("invalid commitment level"),
    };

    assert_eq!(283360248, slot);
    assert_eq!(None, parent);
    assert_eq!(CommitmentLevel::Confirmed, status);
}
