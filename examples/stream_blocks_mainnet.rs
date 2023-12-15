use futures::{Stream, StreamExt};
use log::info;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::pin::pin;

use geyser_grpc_connector::experimental::mock_literpc_core::{map_produced_block, ProducedBlock};
use geyser_grpc_connector::grpc_subscription_autoreconnect::{
    create_geyser_reconnecting_stream, GrpcSourceConfig,
};
use geyser_grpc_connector::grpcmultiplex_fastestwins::{create_multiplex, FromYellowstoneMapper};
use tokio::time::{sleep, Duration};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

fn start_example_consumer(multiplex_stream: impl Stream<Item = ProducedBlock> + Send + 'static) {
    tokio::spawn(async move {
        let mut block_stream = pin!(multiplex_stream);
        while let Some(block) = block_stream.next().await {
            info!("received block #{} from multiplexer", block.slot,);
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
    // RUST_LOG=info,stream_blocks_mainnet=debug,geyser_grpc_connector=trace
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
    let blue_config =
        GrpcSourceConfig::new("mangoams81".to_string(), grpc_addr_mainnet_ams81, None);
    let toxiproxy_config =
        GrpcSourceConfig::new("toxiproxy".to_string(), grpc_addr_mainnet_triton_toxi, None);

    let green_stream =
        create_geyser_reconnecting_stream(green_config.clone(), CommitmentConfig::finalized());
    let blue_stream =
        create_geyser_reconnecting_stream(blue_config.clone(), CommitmentConfig::finalized());
    let toxiproxy_stream =
        create_geyser_reconnecting_stream(toxiproxy_config.clone(), CommitmentConfig::finalized());
    let multiplex_stream = create_multiplex(
        vec![green_stream, blue_stream, toxiproxy_stream],
        CommitmentConfig::finalized(),
        ExtractBlock(CommitmentConfig::finalized()),
    );

    start_example_consumer(multiplex_stream);

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}
