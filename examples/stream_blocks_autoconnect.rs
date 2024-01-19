use futures::{Stream, StreamExt};
use log::info;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::env;
use std::pin::pin;

use geyser_grpc_connector::grpc_subscription_autoreconnect_streams::create_geyser_reconnecting_stream;
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::{
    create_geyser_autoconnection_task, Message,
};
use geyser_grpc_connector::grpcmultiplex_fastestwins::{
    create_multiplexed_stream, FromYellowstoneExtractor,
};
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig};
use tokio::time::{sleep, Duration};
use tracing::warn;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use yellowstone_grpc_proto::prost::Message as _;
use geyser_grpc_connector::channel_plugger::spawn_plugger_mpcs_to_broadcast;

fn start_example_blockmini_consumer(
    multiplex_stream: impl Stream<Item = BlockMini> + Send + 'static,
) {
    tokio::spawn(async move {
        let mut blockmeta_stream = pin!(multiplex_stream);
        while let Some(mini) = blockmeta_stream.next().await {
            info!(
                "emitted block mini #{}@{} with {} bytes from multiplexer",
                mini.slot, mini.commitment_config.commitment, mini.blocksize
            );
        }
    });
}

pub struct BlockMini {
    pub blocksize: usize,
    pub slot: Slot,
    pub commitment_config: CommitmentConfig,
}

struct BlockMiniExtractor(CommitmentConfig);

impl FromYellowstoneExtractor for BlockMiniExtractor {
    type Target = BlockMini;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)> {
        match update.update_oneof {
            Some(UpdateOneof::Block(update_block_message)) => {
                let blocksize = update_block_message.encoded_len();
                let slot = update_block_message.slot;
                let mini = BlockMini {
                    blocksize,
                    slot,
                    commitment_config: self.0,
                };
                Some((slot, mini))
            }
            Some(UpdateOneof::BlockMeta(update_blockmeta_message)) => {
                let blocksize = update_blockmeta_message.encoded_len();
                let slot = update_blockmeta_message.slot;
                let mini = BlockMini {
                    blocksize,
                    slot,
                    commitment_config: self.0,
                };
                Some((slot, mini))
            }
            _ => None,
        }
    }
}

#[warn(dead_code)]
enum TestCases {
    Basic,
    SlowReceiverStartup,
    TemporaryLaggingReceiver,
    CloseAfterReceiving,
    AbortTaskFromOutside,
}
const TEST_CASE: TestCases = TestCases::TemporaryLaggingReceiver;

#[tokio::main]
pub async fn main() {
    // RUST_LOG=info,stream_blocks_mainnet=debug,geyser_grpc_connector=trace
    tracing_subscriber::fmt::init();
    // console_subscriber::init();

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source on {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
    };

    let green_config =
        GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, None, timeouts.clone());

    info!("Write Block stream..");

    let (broadcast_tx, broadcast_rx) = tokio::sync::broadcast::channel(100);
    let (jh_geyser_task, mut message_channel) = create_geyser_autoconnection_task(
        green_config.clone(),
        GeyserFilter(CommitmentConfig::confirmed()).blocks_and_txs(),
    );
    spawn_plugger_mpcs_to_broadcast(message_channel, broadcast_tx);
    let mut message_channel = broadcast_rx;

    tokio::spawn(async move {
        if let TestCases::SlowReceiverStartup = TEST_CASE {
            sleep(Duration::from_secs(5)).await;
        }

        let mut message_count = 0;
        while let Ok(message) = message_channel.recv().await {
            if let TestCases::AbortTaskFromOutside = TEST_CASE {
                if message_count > 5 {
                    info!("(testcase) aborting task from outside");
                    jh_geyser_task.abort();
                }
            }
            match message {
                Message::GeyserSubscribeUpdate(subscriber_update) => {
                    message_count += 1;
                    // info!("got update: {:?}", subscriber_update.update_oneof.);
                    info!("got update!!!");

                    if let TestCases::CloseAfterReceiving = TEST_CASE {
                        info!("(testcase) closing stream after receiving");
                        return;
                    }
                }
                Message::Connecting(attempt) => {
                    warn!("Connection attempt: {}", attempt);
                }
            }

            if let TestCases::TemporaryLaggingReceiver = TEST_CASE {
                if message_count % 3 == 1 {
                    info!("(testcase) lagging a bit");
                    sleep(Duration::from_millis(1500)).await;
                }
            }
        }
        warn!("Stream aborted");
    });

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}
