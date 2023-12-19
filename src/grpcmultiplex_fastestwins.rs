use crate::grpc_subscription_autoreconnect::Message;
use crate::grpc_subscription_autoreconnect::Message::GeyserSubscribeUpdate;
use async_stream::stream;
use futures::Stream;
use log::{debug, info, warn};
use merge_streams::MergeStreams;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use yellowstone_grpc_proto::tonic::codegen::tokio_stream::StreamExt;

pub trait FromYellowstoneExtractor {
    // Target is something like ProducedBlock
    type Target;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)>;
}

struct TaggedMessage {
    pub stream_idx: usize,
    pub payload: Message,
}

/// use streams created by ``create_geyser_reconnecting_stream``
/// note: this is agnostic to the type of the stream
pub fn create_multiplex<M>(
    grpc_source_streams: Vec<impl Stream<Item = Message>>,
    extractor: M,
) -> impl Stream<Item = M::Target>
where
    M: FromYellowstoneExtractor,
{

    if grpc_source_streams.is_empty() {
        panic!("Must have at least one source");
    }

    info!(
        "Starting multiplexer with {} sources",
        grpc_source_streams.len(),
    );

    // use merge
    // let mut futures = futures::stream::SelectAll::new();

    let mut streams = vec![];
    let mut idx = 0;
    for grpc_source in grpc_source_streams {
        let tagged = grpc_source.map(move |msg| TaggedMessage {
            stream_idx: idx,
            payload: msg,
        });
        streams.push(Box::pin(tagged));
        idx += 1;
    }

    let merged_streams = streams.merge();

    extract_payload_from_geyser_updates(merged_streams, extractor)
}

fn extract_payload_from_geyser_updates<E>(merged_streams: impl Stream<Item = TaggedMessage>, extractor: E) -> impl Stream<Item = E::Target>
where
    E: FromYellowstoneExtractor,
{
    let mut tip: Slot = 0;
    stream! {
        for await TaggedMessage {stream_idx, payload} in merged_streams {
            match payload {
                GeyserSubscribeUpdate(update) => {
                    // take only the update messages we want
                    if let Some((proposed_slot, block)) = extractor.map_yellowstone_update(update) {
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
