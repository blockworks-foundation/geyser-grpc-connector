use futures::{Stream, StreamExt};
use log::info;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::env;
use std::pin::pin;

use base64::Engine;
use itertools::Itertools;
use solana_sdk::borsh0_10::try_from_slice_unchecked;
/// This file mocks the core model of the RPC server.
use solana_sdk::{borsh1, compute_budget};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::message::v0::MessageAddressTableLookup;
use solana_sdk::message::{v0, MessageHeader, VersionedMessage};
use solana_sdk::pubkey::Pubkey;

use solana_sdk::signature::Signature;
use solana_sdk::transaction::TransactionError;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;

use geyser_grpc_connector::grpc_subscription_autoreconnect_streams::create_geyser_reconnecting_stream;
use geyser_grpc_connector::grpcmultiplex_fastestwins::{
    create_multiplexed_stream, FromYellowstoneExtractor,
};
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig};
use tokio::time::{sleep, Duration};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

fn start_example_block_consumer(
    multiplex_stream: impl Stream<Item = ProducedBlock> + Send + 'static,
) {
    tokio::spawn(async move {
        let mut block_stream = pin!(multiplex_stream);
        while let Some(block) = block_stream.next().await {
            info!(
                "emitted block #{}@{} from multiplexer",
                block.slot, block.commitment_config.commitment
            );
        }
    });
}

fn start_example_blockmeta_consumer(
    multiplex_stream: impl Stream<Item = BlockMetaMini> + Send + 'static,
) {
    tokio::spawn(async move {
        let mut blockmeta_stream = pin!(multiplex_stream);
        while let Some(mini) = blockmeta_stream.next().await {
            info!(
                "emitted blockmeta #{}@{} from multiplexer",
                mini.slot, mini.commitment_config.commitment
            );
        }
    });
}

struct BlockExtractor(CommitmentConfig);

impl FromYellowstoneExtractor for BlockExtractor {
    type Target = ProducedBlock;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)> {
        match update.update_oneof {
            Some(UpdateOneof::Block(update_block_message)) => {
                let block = map_produced_block(update_block_message, self.0);
                Some((block.slot, block))
            }
            _ => None,
        }
    }
}

pub struct BlockMetaMini {
    pub slot: Slot,
    pub commitment_config: CommitmentConfig,
}

struct BlockMetaExtractor(CommitmentConfig);

impl FromYellowstoneExtractor for BlockMetaExtractor {
    type Target = BlockMetaMini;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)> {
        match update.update_oneof {
            Some(UpdateOneof::BlockMeta(update_blockmeta_message)) => {
                let slot = update_blockmeta_message.slot;
                let mini = BlockMetaMini {
                    slot,
                    commitment_config: self.0,
                };
                Some((slot, mini))
            }
            _ => None,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    // RUST_LOG=info,stream_blocks_mainnet=debug,geyser_grpc_connector=trace
    tracing_subscriber::fmt::init();
    // console_subscriber::init();

    let subscribe_blocks = true;
    let subscribe_blockmeta = false;

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();
    let grpc_addr_blue = env::var("GRPC_ADDR2").expect("need grpc url for blue");
    let grpc_x_token_blue = env::var("GRPC_X_TOKEN2").ok();
    // via toxiproxy
    let grpc_addr_toxiproxy = "http://127.0.0.1:10001".to_string();

    info!(
        "Using green on {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );
    info!(
        "Using blue on {} ({})",
        grpc_addr_blue,
        grpc_x_token_blue.is_some()
    );
    info!("Using toxiproxy on {}", grpc_addr_toxiproxy);

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    let green_config =
        GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, None, timeouts.clone());
    let blue_config =
        GrpcSourceConfig::new(grpc_addr_blue, grpc_x_token_blue, None, timeouts.clone());
    let toxiproxy_config = GrpcSourceConfig::new(grpc_addr_toxiproxy, None, None, timeouts.clone());

    if subscribe_blocks {
        info!("Write Block stream..");
        let green_stream = create_geyser_reconnecting_stream(
            green_config.clone(),
            GeyserFilter(CommitmentConfig::confirmed()).blocks_and_txs(),
        );
        let blue_stream = create_geyser_reconnecting_stream(
            blue_config.clone(),
            GeyserFilter(CommitmentConfig::confirmed()).blocks_and_txs(),
        );
        let toxiproxy_stream = create_geyser_reconnecting_stream(
            toxiproxy_config.clone(),
            GeyserFilter(CommitmentConfig::confirmed()).blocks_and_txs(),
        );
        let multiplex_stream = create_multiplexed_stream(
            vec![green_stream, blue_stream, toxiproxy_stream],
            BlockExtractor(CommitmentConfig::confirmed()),
        );
        start_example_block_consumer(multiplex_stream);
    }

    if subscribe_blockmeta {
        info!("Write BlockMeta stream..");
        let green_stream = create_geyser_reconnecting_stream(
            green_config.clone(),
            GeyserFilter(CommitmentConfig::confirmed()).blocks_meta(),
        );
        let blue_stream = create_geyser_reconnecting_stream(
            blue_config.clone(),
            GeyserFilter(CommitmentConfig::confirmed()).blocks_meta(),
        );
        let toxiproxy_stream = create_geyser_reconnecting_stream(
            toxiproxy_config.clone(),
            GeyserFilter(CommitmentConfig::confirmed()).blocks_meta(),
        );
        let multiplex_stream = create_multiplexed_stream(
            vec![green_stream, blue_stream, toxiproxy_stream],
            BlockMetaExtractor(CommitmentConfig::confirmed()),
        );
        start_example_blockmeta_consumer(multiplex_stream);
    }

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}

#[derive(Default, Debug, Clone)]
pub struct ProducedBlock {
    pub transactions: Vec<TransactionInfo>,
    // pub leader_id: Option<String>,
    pub blockhash: String,
    pub block_height: u64,
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: u64,
    pub commitment_config: CommitmentConfig,
    pub previous_blockhash: String,
    // pub rewards: Option<Vec<Reward>>,
}

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub signature: String,
    pub err: Option<TransactionError>,
    pub cu_requested: Option<u32>,
    pub prioritization_fees: Option<u64>,
    pub cu_consumed: Option<u64>,
    pub recent_blockhash: String,
    pub message: String,
}

pub fn map_produced_block(
    block: SubscribeUpdateBlock,
    commitment_config: CommitmentConfig,
) -> ProducedBlock {
    let txs: Vec<TransactionInfo> = block
        .transactions
        .into_iter()
        .filter_map(|tx| {
            let Some(meta) = tx.meta else {
                return None;
            };

            let Some(transaction) = tx.transaction else {
                return None;
            };

            let Some(message) = transaction.message else {
                return None;
            };

            let Some(header) = message.header else {
                return None;
            };

            let signatures = transaction
                .signatures
                .into_iter()
                .filter_map(|sig| match Signature::try_from(sig) {
                    Ok(sig) => Some(sig),
                    Err(_) => {
                        log::warn!(
                            "Failed to read signature from transaction in block {} - skipping",
                            block.blockhash
                        );
                        None
                    }
                })
                .collect_vec();

            let err = meta.err.map(|x| {
                bincode::deserialize::<TransactionError>(&x.err)
                    .expect("TransactionError should be deserialized")
            });

            let signature = signatures[0];
            let compute_units_consumed = meta.compute_units_consumed;

            let message = VersionedMessage::V0(v0::Message {
                header: MessageHeader {
                    num_required_signatures: header.num_required_signatures as u8,
                    num_readonly_signed_accounts: header.num_readonly_signed_accounts as u8,
                    num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u8,
                },
                account_keys: message
                    .account_keys
                    .into_iter()
                    .map(|key| {
                        let bytes: [u8; 32] =
                            key.try_into().unwrap_or(Pubkey::default().to_bytes());
                        Pubkey::new_from_array(bytes)
                    })
                    .collect(),
                recent_blockhash: Hash::new(&message.recent_blockhash),
                instructions: message
                    .instructions
                    .into_iter()
                    .map(|ix| CompiledInstruction {
                        program_id_index: ix.program_id_index as u8,
                        accounts: ix.accounts,
                        data: ix.data,
                    })
                    .collect(),
                address_table_lookups: message
                    .address_table_lookups
                    .into_iter()
                    .map(|table| {
                        let bytes: [u8; 32] = table
                            .account_key
                            .try_into()
                            .unwrap_or(Pubkey::default().to_bytes());
                        MessageAddressTableLookup {
                            account_key: Pubkey::new_from_array(bytes),
                            writable_indexes: table.writable_indexes,
                            readonly_indexes: table.readonly_indexes,
                        }
                    })
                    .collect(),
            });

            let cu_requested = message
                .instructions()
                .iter()
                .find_map(|i| {
                    if i.program_id(message.static_account_keys())
                        .eq(&compute_budget::id())
                    {
                        if let Ok(ComputeBudgetInstruction::SetComputeUnitLimit(limit)) =
                            borsh1::try_from_slice_unchecked(i.data.as_slice())
                        {
                            return Some(limit);
                        }
                    }
                    None
                });

            let prioritization_fees = message
                .instructions()
                .iter()
                .find_map(|i| {
                    if i.program_id(message.static_account_keys())
                        .eq(&compute_budget::id())
                    {
                        if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) =
                            borsh1::try_from_slice_unchecked(i.data.as_slice())
                        {
                            return Some(price);
                        }
                    }

                    None
                });

            Some(TransactionInfo {
                signature: signature.to_string(),
                err,
                cu_requested,
                prioritization_fees,
                cu_consumed: compute_units_consumed,
                recent_blockhash: message.recent_blockhash().to_string(),
                message: base64::engine::general_purpose::STANDARD.encode(message.serialize()),
            })
        })
        .collect();

    // removed rewards

    ProducedBlock {
        transactions: txs,
        block_height: block
            .block_height
            .map(|block_height| block_height.block_height)
            .unwrap(),
        block_time: block.block_time.map(|time| time.timestamp).unwrap() as u64,
        blockhash: block.blockhash,
        previous_blockhash: block.parent_blockhash,
        commitment_config,
        // leader_id,
        parent_slot: block.parent_slot,
        slot: block.slot,
        // rewards,
    }
}
