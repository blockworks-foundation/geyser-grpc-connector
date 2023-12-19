use std::pin::pin;
use log::{debug, info, trace};
use crate::grpc_subscription_autoreconnect::Message;
use crate::grpc_subscription_autoreconnect::Message::GeyserSubscribeUpdate;
use async_stream::stream;
use futures::{Stream, StreamExt};
use merge_streams::MergeStreams;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::spawn;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use crate::experimental::mock_literpc_core::ProducedBlock;

pub async fn channelize_stream<T>(source: impl Stream<Item = T> + Send + 'static) -> (Receiver<T>, JoinHandle<()>)
where
    T: Clone + Send + Sync + 'static,
{
    let (tx, multiplexed_finalized_blocks) = tokio::sync::broadcast::channel::<T>(1000);

    let jh_channelizer = spawn(async move {
        let mut block_stream = pin!(source);
        'main_loop: while let Some(block) = block_stream.next().await {
            debug!("multiplex -> ...");

            match tx.send(block) {
                Ok(receivers) => {
                    trace!("sent data to {} receivers", receivers);
                }
                Err(send_error) => {
                    match send_error {
                        SendError(_) => {
                            debug!("no active blockreceivers - skipping message");
                            continue 'main_loop;
                        }
                    }
                }
            };
        }
        panic!("forward task failed");
    });

    (multiplexed_finalized_blocks, jh_channelizer)
}