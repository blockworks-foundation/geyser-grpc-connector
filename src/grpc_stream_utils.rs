use std::pin::pin;
use log::{debug, info};
use crate::grpc_subscription_autoreconnect::Message;
use crate::grpc_subscription_autoreconnect::Message::GeyserSubscribeUpdate;
use async_stream::stream;
use futures::{Stream, StreamExt};
use merge_streams::MergeStreams;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::broadcast::error::SendError;
use tokio::task::JoinHandle;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use crate::experimental::mock_literpc_core::ProducedBlock;

// pub async fn forward_to_channel<T: Clone>(multiplex_stream: impl Stream<Item = T> + Send + 'static) -> JoinHandle<()> {
//     let (block_sx, blocks_notifier) = tokio::sync::broadcast::channel(10);
//     tokio::task::spawn(async move {
//         let mut block_stream = pin!(multiplex_stream);
//         'main_loop: while let Some(block) = block_stream.next().await {
//             match block_sx.send(block) {
//                 Ok(receivers) => {
//                     debug!("sent block to {} receivers", receivers);
//                 }
//                 Err(send_error) => {
//                     match send_error {
//                         SendError(_) => {
//                             info!("Stop sending blocks on stream - shutting down");
//                             break 'main_loop;
//                         }
//                     }
//                 }
//             };
//         }
//         panic!("forward task failed");
//     })
//
// }