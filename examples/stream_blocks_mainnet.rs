use std::collections::HashMap;
use std::pin::pin;
use futures::{Stream, StreamExt};
use log::{info};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::broadcast::{Receiver};
use tokio::time::{sleep, Duration};
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeUpdate, SubscribeUpdateBlock};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use geyser_grpc_connector::grpc_subscription_autoreconnect::GrpcSourceConfig;
use geyser_grpc_connector::grpcmultiplex_fastestwins::{create_multiplex, FromYellowstoneMapper};
use geyser_grpc_connector::experimental::mock_literpc_core::{map_produced_block, ProducedBlock};

fn start_example_consumer(mut block_stream: impl Stream<Item=ProducedBlock> + Send + 'static) {
    tokio::spawn(async move {
        let mut block_stream = pin!(block_stream);
        while let Some(block) = block_stream.next().await {
            info!("received block #{}", block.slot,);
        }
    });
}


struct ExtractBlock(CommitmentConfig);
impl FromYellowstoneMapper for ExtractBlock {
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


#[tokio::main]
pub async fn main() {
    // RUST_LOG=info,grpc_using_streams=debug
    tracing_subscriber::fmt::init();
    // console_subscriber::init();

    // mango validator (mainnet)
    let grpc_addr_mainnet_triton = "http://202.8.9.108:10000".to_string();
    // via toxiproxy
    let grpc_addr_mainnet_triton_toxi = "http://127.0.0.1:10001".to_string();
    // ams81 (mainnet)
    let grpc_addr_mainnet_ams81 = "http://202.8.8.12:10000".to_string();
    // testnet - NOTE: this connection has terrible lags (almost 5 minutes)
    // let grpc_addr = "http://147.28.169.13:10000".to_string();


    let green_config = GrpcSourceConfig::new("triton".to_string(), grpc_addr_mainnet_triton, None);
    let blue_config = GrpcSourceConfig::new("mangoams81".to_string(), grpc_addr_mainnet_ams81, None);
    let toxiproxy_config = GrpcSourceConfig::new("toxiproxy".to_string(), grpc_addr_mainnet_triton_toxi, None);

    let multiplex_stream = create_multiplex(
        vec![green_config, blue_config, toxiproxy_config],
        CommitmentConfig::finalized(),
        ExtractBlock(CommitmentConfig::confirmed()),);

    start_example_consumer(multiplex_stream);

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;

}

