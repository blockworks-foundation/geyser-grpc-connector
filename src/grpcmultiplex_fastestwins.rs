use crate::grpc_subscription_autoreconnect::Message;
use crate::grpc_subscription_autoreconnect::Message::GeyserSubscribeUpdate;
use async_stream::stream;
use futures::Stream;
use log::{info, warn};
use merge_streams::MergeStreams;
use solana_sdk::clock::Slot;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use yellowstone_grpc_proto::tonic::codegen::tokio_stream::StreamExt;

pub trait FromYellowstoneExtractor {
    // Target is something like ProducedBlock
    type Target;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)>;
}

/// use streams created by ``create_geyser_reconnecting_stream``
/// this is agnostic to the type of the stream
/// CAUTION: do not try to use with commitment level "processed" as this will form trees (forks) and not a sequence
pub fn create_multiplexed_stream<E>(
    grpc_source_streams: Vec<impl Stream<Item = Message>>,
    extractor: E,
) -> impl Stream<Item = E::Target>
where
    E: FromYellowstoneExtractor,
{
    if grpc_source_streams.is_empty() {
        panic!("Must have at least one grpc source");
    }

    info!(
        "Starting multiplexer with {} sources",
        grpc_source_streams.len()
    );

    let mut streams = vec![];
    for (idx, grpc_stream) in grpc_source_streams.into_iter().enumerate() {
        let tagged = grpc_stream.map(move |msg| TaggedMessage {
            stream_idx: idx,
            payload: msg,
        });
        streams.push(Box::pin(tagged));
    }

    let merged_streams = streams.merge();

    extract_payload_from_geyser_updates(merged_streams, extractor)
}

struct TaggedMessage {
    pub stream_idx: usize,
    pub payload: Message,
}

fn extract_payload_from_geyser_updates<E>(
    merged_stream: impl Stream<Item = TaggedMessage>,
    extractor: E,
) -> impl Stream<Item = E::Target>
where
    E: FromYellowstoneExtractor,
{
    let mut tip: Slot = 0;
    stream! {
        for await TaggedMessage {stream_idx, payload} in merged_stream {
            match payload {
                GeyserSubscribeUpdate(update) => {
                    // take only the update messages we want
                    if let Some((proposed_slot, block)) = extractor.map_yellowstone_update(*update) {
                        if proposed_slot > tip {
                            tip = proposed_slot;
                            yield block;
                        }
                    }
                }
                Message::Connecting(attempt) => {
                    if attempt > 1 {
                        warn!("Stream-{} performs reconnect attempt {}", stream_idx, attempt);
                    }
                }
            }
        }
    }
}
