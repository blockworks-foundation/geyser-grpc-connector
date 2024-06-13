use futures::{Stream, StreamExt};
use log::info;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::env;
use std::pin::pin;
use solana_sdk::pubkey::Pubkey;

use geyser_grpc_connector::grpc_subscription_autoreconnect_streams::create_geyser_reconnecting_stream;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use tokio::time::{sleep, Duration};
use tracing::warn;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use yellowstone_grpc_proto::prost::Message as _;

#[allow(dead_code)]
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

#[tokio::main]
pub async fn main() {
    // RUST_LOG=info,stream_blocks_mainnet=debug,geyser_grpc_connector=trace
    tracing_subscriber::fmt::init();
    // console_subscriber::init();

    const COMMITMENT_LEVEL: CommitmentConfig = CommitmentConfig::processed();
    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source on {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(25),
        request_timeout: Duration::from_secs(25),
        subscribe_timeout: Duration::from_secs(25),
        receive_timeout: Duration::from_secs(25),
    };

    let config = GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, None, timeouts.clone());

    info!("Write Block stream..");

    let green_stream = create_geyser_reconnecting_stream(
        config.clone(),
        GeyserFilter(COMMITMENT_LEVEL).accounts(),
    );

    let blue_stream = create_geyser_reconnecting_stream(
        config.clone(),
        GeyserFilter(COMMITMENT_LEVEL).blocks_and_txs(),
    );

    tokio::spawn(async move {
        let mut green_stream = pin!(green_stream);
        while let Some(message) = green_stream.next().await {
            match message {
                Message::GeyserSubscribeUpdate(subscriber_update) => {
                    match subscriber_update.update_oneof {
                        Some(UpdateOneof::Account(update)) => {
                            let account_info = update.account.unwrap();
                            let account_pk = Pubkey::try_from(account_info.pubkey).unwrap();
                            info!("got account update (green)!!! {} - {:?} - {} bytes",
                                update.slot, account_pk, account_info.data.len());
                            let bytes: [u8; 32] =
                                account_pk.to_bytes();
                        }
                        _ => {}
                    }
                }
                Message::Connecting(attempt) => {
                    warn!("Connection attempt: {}", attempt);
                }
            }
        }
        warn!("Stream aborted");
    });

    tokio::spawn(async move {
        let mut blue_stream = pin!(blue_stream);
        let extractor = BlockMiniExtractor(COMMITMENT_LEVEL);
        while let Some(message) = blue_stream.next().await {
            match message {
                Message::GeyserSubscribeUpdate(subscriber_update) => {
                    let mapped = extractor.map_yellowstone_update(*subscriber_update);
                    if let Some((slot, block_mini)) = mapped {
                        info!("got update (blue)!!! block: {} - {} bytes", slot, block_mini.blocksize);
                    }
                }
                Message::Connecting(attempt) => {
                    warn!("Connection attempt: {}", attempt);
                }
            }
        }
        warn!("Stream aborted");
    });

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;
}

fn map_block_update(update: SubscribeUpdate) -> Option<Slot> {
    match update.update_oneof {
        Some(UpdateOneof::Block(update_block_message)) => {
            let slot = update_block_message.slot;
            Some(slot)
        }
        _ => None,
    }
}
