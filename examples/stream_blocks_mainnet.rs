use std::env;
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
            info!("emitted block #{} from multiplexer", block.slot);
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

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();
    let grpc_addr_blue = env::var("GRPC_ADDR2").expect("need grpc url for blue");
    let grpc_x_token_blue = env::var("GRPC_X_TOKEN2").ok();
    // via toxiproxy
    let grpc_addr_toxiproxy = "http://127.0.0.1:10001".to_string();

    info!("Using green on {} ({})", grpc_addr_green, grpc_x_token_green.is_some());
    info!("Using blue on {} ({})", grpc_addr_blue, grpc_x_token_blue.is_some());
    info!("Using toxiproxy on {}", grpc_addr_toxiproxy);

    let green_config = GrpcSourceConfig::new("greensource".to_string(), grpc_addr_green, grpc_x_token_green);
    let blue_config =
        GrpcSourceConfig::new("bluesource".to_string(), grpc_addr_blue, grpc_x_token_blue);
    let toxiproxy_config =
        GrpcSourceConfig::new("toxiproxy".to_string(), grpc_addr_toxiproxy, None);


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
