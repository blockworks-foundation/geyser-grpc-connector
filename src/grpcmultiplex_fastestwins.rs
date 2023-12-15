use async_stream::stream;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use log::{debug, info, warn};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::pin::{pin, Pin};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, GeyserGrpcClientResult};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlockMeta;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequestFilterBlocks, SubscribeUpdate,
};
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterBlocksMeta;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::tonic::{async_trait, Status};
use crate::grpc_subscription_autoreconnect::{create_geyser_reconnecting_stream, GrpcSourceConfig};


pub trait FromYellowstoneMapper {
    // Target is something like ProducedBlock
    type Target;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)>;
}

struct ExtractBlock(CommitmentConfig);

struct ExtractBlockMeta(CommitmentConfig);


pub fn create_multiplex<E>(
    // TODO provide list of streams
    grpc_sources: Vec<GrpcSourceConfig>,
    commitment_config: CommitmentConfig,
    extractor: E,
) -> impl Stream<Item = E::Target>
    where
        E: FromYellowstoneMapper,
{
    assert!(
        commitment_config == CommitmentConfig::confirmed()
            || commitment_config == CommitmentConfig::finalized(),
        "Only CONFIRMED and FINALIZED is supported");
    // note: PROCESSED blocks are not sequential in presense of forks; this will break the logic

    if grpc_sources.len() < 1 {
        panic!("Must have at least one source");
    }

    info!(
        "Starting multiplexer with {} sources: {}",
        grpc_sources.len(),
        grpc_sources
            .iter()
            .map(|source| source.label.clone())
            .join(", ")
    );

    let mut futures = futures::stream::SelectAll::new();

    for grpc_source in grpc_sources {
        futures.push(Box::pin(create_geyser_reconnecting_stream(
            grpc_source.clone(),
            commitment_config,
        )));
    }

    map_updates(futures, extractor)
}

fn map_updates<S, E>(geyser_stream: S, mapper: E) -> impl Stream<Item = E::Target>
    where
        S: Stream<Item = Option<SubscribeUpdate>>,
        E: FromYellowstoneMapper,
{
    let mut tip: Slot = 0;
    stream! {
        for await update in geyser_stream {
            match update {
                Some(update) => {
                    // take only the update messages we want
                    if let Some((proposed_slot, block)) = mapper.map_yellowstone_update(update) {
                        if proposed_slot > tip {
                            tip = proposed_slot;
                            yield block;
                        }
                    }
                }
                None => {
                    debug!("Stream sent None"); // TODO waht does that mean?
                }
            }
        }
    }
}

