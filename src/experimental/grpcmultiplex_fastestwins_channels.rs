/// Deprecated task/channel-based implementation of the multiplexer - use the stream-based one instead


use std::collections::{HashMap};
use std::ops::{Add};
use anyhow::{Context};
use async_stream::stream;
use futures::{Stream, StreamExt};
use itertools::{Itertools};
use log::{debug, info, warn};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::commitment_config::CommitmentLevel;
use tokio::{select};
use tokio::sync::broadcast::{Sender};
use tokio::time::{Duration, Instant, sleep_until};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{SubscribeRequestFilterBlocks, SubscribeUpdate, SubscribeUpdateBlock};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;

// use solana_lite_rpc_cluster_endpoints::grpc_subscription::{create_block_processing_task, map_produced_block};
// use solana_lite_rpc_core::AnyhowJoinHandle;
// use solana_lite_rpc_core::structures::produced_block::ProducedBlock;

// TODO map SubscribeUpdateBlock
pub async fn create_multiplex(
    grpc_sources: Vec<GrpcSourceConfig>,
    commitment_config: CommitmentConfig,
    block_sx: Sender<Box<SubscribeUpdateBlock>>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    assert!(
        commitment_config == CommitmentConfig::confirmed()
            || commitment_config == CommitmentConfig::finalized(),
        "Only CONFIRMED and FINALIZED is supported");
    // note: PROCESSED blocks are not sequential in presense of forks; this will break the logic

    if grpc_sources.is_empty() {
        panic!("Must have at least one source");
    }

    tokio::spawn(async move {
        info!("Starting multiplexer with {} sources: {}",
            grpc_sources.len(),
            grpc_sources.iter().map(|source| source.label.clone()).join(", "));

        let mut futures = futures::stream::SelectAll::new();
        for grpc_source in grpc_sources {
            // note: stream never terminates
            let stream = create_geyser_reconnecting_stream(grpc_source.clone(), commitment_config).await;
            futures.push(Box::pin(stream));
        }

        let mut current_slot: Slot = 0;

        'main_loop: loop {

            let block_cmd = select! {
                message = futures.next() => {
                    match message {
                        Some(message) => {
                            map_filter_block_message(current_slot, message, commitment_config)
                        }
                        None => {
                            panic!("source stream is not supposed to terminate");
                        }
                    }
                }
            };

            match block_cmd {
                BlockCmd::ForwardBlock(block) => {
                    current_slot = block.slot;
                    block_sx.send(block).context("send block to downstream")?;
                }
                BlockCmd::DiscardBlockBehindTip(slot) => {
                    debug!(". discarding redundant block #{}", slot);
                }
                BlockCmd::SkipMessage => {
                    debug!(". skipping this message by type");
                }
            }

        }
    })
}

// look Ma, no Clone!
#[derive(Debug)]
enum BlockCmd {
    ForwardBlock(Box<SubscribeUpdateBlock>),
    DiscardBlockBehindTip(Slot),
    // skip geyser messages which are not block related updates
    SkipMessage,
}

fn map_filter_block_message(current_slot: Slot, update_message: SubscribeUpdate, _commitment_config: CommitmentConfig) -> BlockCmd {
    if let Some(UpdateOneof::Block(update_block_message)) = update_message.update_oneof {
        if update_block_message.slot <= current_slot && current_slot != 0 {
            // no progress - skip this
            return BlockCmd::DiscardBlockBehindTip(update_block_message.slot);
        }

        // expensive
        // let produced_block = map_produced_block(update_block_message, commitment_config);

        BlockCmd::ForwardBlock(Box::new(update_block_message))
    } else {
        BlockCmd::SkipMessage
    }

}

#[derive(Clone, Debug)]
pub struct GrpcSourceConfig {
    // symbolic name used in logs
    label: String,
    grpc_addr: String,
    grpc_x_token: Option<String>,
    tls_config: Option<ClientTlsConfig>,
}

impl GrpcSourceConfig {
    pub fn new(label: String, grpc_addr: String, grpc_x_token: Option<String>) -> Self {
        Self {
            label,
            grpc_addr,
            grpc_x_token,
            tls_config: None,
        }
    }
}

// TODO use GrpcSource
// note: stream never terminates
async fn create_geyser_reconnecting_stream(
    grpc_source: GrpcSourceConfig,
    commitment_config: CommitmentConfig) -> impl Stream<Item = SubscribeUpdate> {

    // solana_sdk -> yellowstone
    let commitment_level = match commitment_config.commitment {
        solana_sdk::commitment_config::CommitmentLevel::Confirmed => yellowstone_grpc_proto::prelude::CommitmentLevel::Confirmed,
        solana_sdk::commitment_config::CommitmentLevel::Finalized => yellowstone_grpc_proto::prelude::CommitmentLevel::Finalized,
        _ => panic!("Only CONFIRMED and FINALIZED is supported/suggested"),
    };

    let label = grpc_source.label.clone();
    stream! {
        let mut throttle_barrier = Instant::now();
        'reconnect_loop: loop {
            sleep_until(throttle_barrier).await;
            throttle_barrier = Instant::now().add(Duration::from_millis(1000));

            let connect_result = GeyserGrpcClient::connect_with_timeout(
                grpc_source.grpc_addr.clone(), grpc_source.grpc_x_token.clone(), grpc_source.tls_config.clone(),
                Some(Duration::from_secs(2)), Some(Duration::from_secs(2)), false).await;

            let mut client = match connect_result {
                Ok(connected_client) => connected_client,
                Err(geyser_grpc_client_error) => {
                    // TODO identify non-recoverable errors and cancel stream
                    warn!("Connect failed on {} - retrying: {:?}", label, geyser_grpc_client_error);
                    continue 'reconnect_loop;
                }
            };

            let mut blocks_subs = HashMap::new();
            blocks_subs.insert(
                "client".to_string(),
                SubscribeRequestFilterBlocks {
                    account_include: Default::default(),
                    include_transactions: Some(true),
                    include_accounts: Some(false),
                    include_entries: Some(false),
                },
            );

            let subscribe_result = client
                .subscribe_once(
                    HashMap::new(),
                    Default::default(),
                    HashMap::new(),
                    Default::default(),
                    blocks_subs,
                    Default::default(),
                    Some(commitment_level),
                    Default::default(),
                    None,
                ).await;

            let geyser_stream = match subscribe_result {
                Ok(subscribed_stream) => subscribed_stream,
                Err(geyser_grpc_client_error) => {
                    // TODO identify non-recoverable errors and cancel stream
                    warn!("Subscribe failed on {} - retrying: {:?}", label, geyser_grpc_client_error);
                    continue 'reconnect_loop;
                }
            };

            for await update_message in geyser_stream {
                match update_message {
                    Ok(update_message) => {
                        info!(">message on {}", label);
                        yield update_message;
                    }
                    Err(tonic_status) => {
                        // TODO identify non-recoverable errors and cancel stream
                        warn!("Receive error on {} - retrying: {:?}", label, tonic_status);
                        continue 'reconnect_loop;
                    }
                }
            } // -- production loop

            warn!("stream consumer loop {} terminated", label);
        } // -- main loop
    } // -- stream!

}
