use std::env;
use futures::{Stream, StreamExt};
use log::info;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::pin::pin;

use geyser_grpc_connector::experimental::mock_literpc_core::{map_produced_block, ProducedBlock};
use geyser_grpc_connector::grpc_subscription_autoreconnect::{create_geyser_reconnecting_stream, GrpcConnectionTimeouts, GrpcSourceConfig};
use geyser_grpc_connector::grpcmultiplex_fastestwins::{create_multiplex, FromYellowstoneMapper};
use tokio::time::{sleep, Duration};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

fn start_example_block_consumer(multiplex_stream: impl Stream<Item = ProducedBlock> + Send + 'static) {
    tokio::spawn(async move {
        let mut block_stream = pin!(multiplex_stream);
        while let Some(block) = block_stream.next().await {
            info!("emitted block #{}@{} from multiplexer", block.slot, block.commitment_config.commitment);
        }
    });
}

fn start_example_blockmeta_consumer(multiplex_stream: impl Stream<Item = BlockMetaMini> + Send + 'static) {
    tokio::spawn(async move {
        let mut blockmeta_stream = pin!(multiplex_stream);
        while let Some(mini) = blockmeta_stream.next().await {
            info!("emitted blockmeta #{}@{} from multiplexer", mini.slot, mini.commitment_config.commitment);
        }
    });
}

struct BlockExtractor(CommitmentConfig);

impl FromYellowstoneMapper for BlockExtractor {
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

impl FromYellowstoneMapper for BlockMetaExtractor {
    type Target = BlockMetaMini;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)> {
        match update.update_oneof {
            Some(UpdateOneof::Block(update_blockmeta_message)) => {
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

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();
    let grpc_addr_blue = env::var("GRPC_ADDR2").expect("need grpc url for blue");
    let grpc_x_token_blue = env::var("GRPC_X_TOKEN2").ok();
    // via toxiproxy
    let grpc_addr_toxiproxy = "http://127.0.0.1:10001".to_string();

    info!("Using green on {} ({})", grpc_addr_green, grpc_x_token_green.is_some());
    info!("Using blue on {} ({})", grpc_addr_blue, grpc_x_token_blue.is_some());
    info!("Using toxiproxy on {}", grpc_addr_toxiproxy);

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
    };

    let green_config = GrpcSourceConfig::new_with_timeout("greensource".to_string(), grpc_addr_green, grpc_x_token_green, timeouts.clone());
    let blue_config =
        GrpcSourceConfig::new_with_timeout("bluesource".to_string(), grpc_addr_blue, grpc_x_token_blue, timeouts.clone());
    let toxiproxy_config =
        GrpcSourceConfig::new_with_timeout("toxiproxy".to_string(), grpc_addr_toxiproxy, None, timeouts.clone());

    {
        info!("Write Block stream..");
        let green_stream =
            create_geyser_reconnecting_stream(green_config.clone(), CommitmentConfig::finalized());
        let blue_stream =
            create_geyser_reconnecting_stream(blue_config.clone(), CommitmentConfig::finalized());
        let toxiproxy_stream =
            create_geyser_reconnecting_stream(toxiproxy_config.clone(), CommitmentConfig::finalized());
        let multiplex_stream = create_multiplex(
            vec![green_stream, blue_stream, toxiproxy_stream],
            CommitmentConfig::finalized(),
            BlockExtractor(CommitmentConfig::finalized()),
        );
        start_example_block_consumer(multiplex_stream);
    }

    {
        info!("Write BlockMeta stream..");
        let green_stream =
            create_geyser_reconnecting_stream(green_config.clone(), CommitmentConfig::finalized());
        let blue_stream =
            create_geyser_reconnecting_stream(blue_config.clone(), CommitmentConfig::finalized());
        let toxiproxy_stream =
            create_geyser_reconnecting_stream(toxiproxy_config.clone(), CommitmentConfig::finalized());
        let multiplex_stream = create_multiplex(
            vec![green_stream, blue_stream, toxiproxy_stream],
            CommitmentConfig::finalized(),
            BlockMetaExtractor(CommitmentConfig::finalized()),
        );
        start_example_blockmeta_consumer(multiplex_stream);
    }

        // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}
