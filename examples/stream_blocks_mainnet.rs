use futures::{Stream, StreamExt};
use log::info;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::env;
use std::pin::pin;

use geyser_grpc_connector::experimental::mock_literpc_core::{map_produced_block, ProducedBlock};
use geyser_grpc_connector::grpc_subscription_autoreconnect::{
    create_geyser_reconnecting_stream, GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig,
};
use geyser_grpc_connector::grpcmultiplex_fastestwins::{
    create_multiplexed_stream, FromYellowstoneExtractor,
};
use tokio::time::{sleep, Duration};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

fn start_example_block_consumer(
    multiplex_stream: impl Stream<Item = ProducedBlock> + Send + 'static,
) {
    tokio::spawn(async move {
        let mut block_stream = pin!(multiplex_stream);
        while let Some(block) = block_stream.next().await {
            info!(
                "emitted block #{}@{} from multiplexer",
                block.slot, block.commitment_config.commitment
            );
        }
    });
}

fn start_example_blockmeta_consumer(
    multiplex_stream: impl Stream<Item = BlockMetaMini> + Send + 'static,
) {
    tokio::spawn(async move {
        let mut blockmeta_stream = pin!(multiplex_stream);
        while let Some(mini) = blockmeta_stream.next().await {
            info!(
                "emitted blockmeta #{}@{} from multiplexer",
                mini.slot, mini.commitment_config.commitment
            );
        }
    });
}

struct BlockExtractor(CommitmentConfig);

impl FromYellowstoneExtractor for BlockExtractor {
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

pub struct BlockMetaMini {
    pub slot: Slot,
    pub commitment_config: CommitmentConfig,
}

struct BlockMetaExtractor(CommitmentConfig);

impl FromYellowstoneExtractor for BlockMetaExtractor {
    type Target = BlockMetaMini;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)> {
        match update.update_oneof {
            Some(UpdateOneof::BlockMeta(update_blockmeta_message)) => {
                let slot = update_blockmeta_message.slot;
                let mini = BlockMetaMini {
                    slot,
                    commitment_config: self.0,
                };
                Some((slot, mini))
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

    let subscribe_blocks = true;
    let subscribe_blockmeta = false;

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();
    let grpc_addr_blue = env::var("GRPC_ADDR2").expect("need grpc url for blue");
    let grpc_x_token_blue = env::var("GRPC_X_TOKEN2").ok();
    // via toxiproxy
    let grpc_addr_toxiproxy = "http://127.0.0.1:10001".to_string();

    info!(
        "Using green on {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );
    info!(
        "Using blue on {} ({})",
        grpc_addr_blue,
        grpc_x_token_blue.is_some()
    );
    info!("Using toxiproxy on {}", grpc_addr_toxiproxy);

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
    };

    let green_config =
        GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, None, timeouts.clone());
    let blue_config =
        GrpcSourceConfig::new(grpc_addr_blue, grpc_x_token_blue, None, timeouts.clone());
    let toxiproxy_config = GrpcSourceConfig::new(grpc_addr_toxiproxy, None, None, timeouts.clone());

    if subscribe_blocks {
        info!("Write Block stream..");
        let green_stream = create_geyser_reconnecting_stream(
            green_config.clone(),
            GeyserFilter::blocks_and_txs(),
            CommitmentConfig::confirmed(),
        );
        let blue_stream = create_geyser_reconnecting_stream(
            blue_config.clone(),
            GeyserFilter::blocks_and_txs(),
            CommitmentConfig::confirmed(),
        );
        let toxiproxy_stream = create_geyser_reconnecting_stream(
            toxiproxy_config.clone(),
            GeyserFilter::blocks_and_txs(),
            CommitmentConfig::confirmed(),
        );
        let multiplex_stream = create_multiplexed_stream(
            vec![green_stream, blue_stream, toxiproxy_stream],
            BlockExtractor(CommitmentConfig::confirmed()),
        );
        start_example_block_consumer(multiplex_stream);
    }

    if subscribe_blockmeta {
        info!("Write BlockMeta stream..");
        let green_stream = create_geyser_reconnecting_stream(
            green_config.clone(),
            GeyserFilter::blocks_meta(),
            CommitmentConfig::confirmed(),
        );
        let blue_stream = create_geyser_reconnecting_stream(
            blue_config.clone(),
            GeyserFilter::blocks_meta(),
            CommitmentConfig::confirmed(),
        );
        let toxiproxy_stream = create_geyser_reconnecting_stream(
            toxiproxy_config.clone(),
            GeyserFilter::blocks_meta(),
            CommitmentConfig::confirmed(),
        );
        let multiplex_stream = create_multiplexed_stream(
            vec![green_stream, blue_stream, toxiproxy_stream],
            BlockMetaExtractor(CommitmentConfig::confirmed()),
        );
        start_example_blockmeta_consumer(multiplex_stream);
    }

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}
