use log::info;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::env;
use tokio::sync::broadcast;

use geyser_grpc_connector::channel_plugger::spawn_broadcast_channel_plug;
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use tokio::time::{sleep, Duration};
use tracing::warn;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use yellowstone_grpc_proto::prost::Message as _;

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

#[allow(dead_code)]
enum TestCases {
    Basic,
    SlowReceiverStartup,
    TemporaryLaggingReceiver,
    CloseAfterReceiving,
    AbortTaskFromOutside,
}
const TEST_CASE: TestCases = TestCases::Basic;

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
        receive_timeout: Duration::from_secs(5),
    };

    let green_config =
        GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, None, timeouts.clone());

    info!("Write Block stream..");

    let (_, exit_notify) = broadcast::channel(1);

    let (jh_geyser_task, message_channel) = create_geyser_autoconnection_task(
        green_config.clone(),
        GeyserFilter(CommitmentConfig::confirmed()).blocks_and_txs(),
        exit_notify,
        false,
    );
    let mut message_channel =
        spawn_broadcast_channel_plug(tokio::sync::broadcast::channel(8), message_channel);

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
                    info!("got update - {} bytes", subscriber_update.encoded_len());

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
    sleep(Duration::from_secs(2000)).await;
}
