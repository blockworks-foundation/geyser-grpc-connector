use crate::grpc_subscription_autoreconnect::{create_geyser_reconnecting_stream, GrpcSourceConfig};
use async_stream::stream;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use log::{debug, info};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

pub trait FromYellowstoneMapper {
    // Target is something like ProducedBlock
    type Target;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)>;
}

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
        "Only CONFIRMED and FINALIZED is supported"
    );
    // note: PROCESSED blocks are not sequential in presense of forks; this will break the logic

    if grpc_sources.is_empty() {
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
