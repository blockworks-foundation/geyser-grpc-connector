// use crate::{
//     endpoint_stremers::EndpointStreaming,
//     rpc_polling::vote_accounts_and_cluster_info_polling::poll_vote_accounts_and_cluster_info,
// };
use anyhow::{bail, Context};
use futures::StreamExt;

use merge_streams::MergeStreams;
use std::collections::HashMap;
use tokio::sync::broadcast::Sender;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequestFilterBlocks,
    SubscribeUpdateBlock,
}, geyser::SlotParallelization};

pub fn create_block_processing_task(
    grpc_addr: String,
    grpc_x_token: Option<String>,
    block_sx: Sender<SubscribeUpdateBlock>,
    commitment_level: CommitmentLevel,
    parallelization: i32,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {

    tokio::spawn(async move {
        // connect to grpc
        let mut client = GeyserGrpcClient::connect(grpc_addr, grpc_x_token, None)?;
        let mut stream = if parallelization > 1 {
            let mut blocks_subs = HashMap::new();
            blocks_subs.insert(
                "client".to_string(),
                SubscribeRequestFilterBlocks {
                    account_include: Default::default(),
                    include_transactions: Some(true),
                    include_accounts: Some(false),
                    include_entries: Some(false),
                    slot_parallelization: None,
                },
            );
            let s = client
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
            )
            .await?;
            vec![s].merge()
        } else {
            let mut streams = vec![];
            for filter_id in 0..parallelization {
                let mut blocks_subs = HashMap::new();
                blocks_subs.insert(
                    format!("block_stream_{}", filter_id),
                    SubscribeRequestFilterBlocks {
                        account_include: Default::default(),
                        include_transactions: Some(true),
                        include_accounts: Some(false),
                        include_entries: Some(false),
                        slot_parallelization: Some(SlotParallelization {
                            filter_id,
                            filter_size: parallelization
                        })
                    },
                );
                streams.push(client
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
                    )
                    .await?);
            }
            streams.merge()
        };

        while let Some(message) = stream.next().await {
            let message = message?;

            let Some(update) = message.update_oneof else {
                continue;
            };

            match update {
                UpdateOneof::Block(block) => {
                    // let block = map_produced_block(block, commitment_config);

                    block_sx
                        .send(block)
                        .context("Grpc failed to send a block")?;
                }
                UpdateOneof::Ping(_) => {
                    log::trace!("GRPC Ping");
                }
                u => {
                    bail!("Unexpected update: {u:?}");
                }
            };
        }
        bail!("geyser slot stream ended");
    })
}
