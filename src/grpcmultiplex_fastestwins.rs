use async_stream::stream;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use log::{info, warn};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::pin::{pin, Pin};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, GeyserGrpcClientResult};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlockMeta;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequestFilterBlocks, SubscribeUpdate,
};
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterBlocksMeta;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::tonic::{async_trait, Status};


#[async_trait]
trait GrpcConnectionFactory: Clone {
    // async fn connect() -> GeyserGrpcClientResult<impl Stream<Item=Result<SubscribeUpdate, Status>>+Sized>;
    async fn connect_and_subscribe(&self) -> GeyserGrpcClientResult<Pin<Box<dyn Stream<Item=Result<SubscribeUpdate, Status>>>>>;
}

#[derive(Clone)]
struct SampleConnector {
    grpc_addr: String,
    grpc_x_token: Option<String>,
    tls_config: Option<ClientTlsConfig>,
}

#[async_trait]
impl GrpcConnectionFactory for SampleConnector {
    async fn connect_and_subscribe(&self) -> GeyserGrpcClientResult<Pin<Box<dyn Stream<Item=Result<SubscribeUpdate, Status>>>>> {
        let mut client = GeyserGrpcClient::connect_with_timeout(
            self.grpc_addr.clone(),
            self.grpc_x_token.clone(),
            self.tls_config.clone(),
            Some(Duration::from_secs(2)),
            Some(Duration::from_secs(2)),
            false,
        ).await?;

        let subscribe_result = client
            .subscribe_once(
                HashMap::new(),
                Default::default(),
                HashMap::new(),
                Default::default(),
                HashMap::new(),
                HashMap::new(),
                Some(CommitmentLevel::Finalized),
                Default::default(),
                None,
            ).await?;

        Ok(Box::pin(subscribe_result))
    }

}

pub trait FromYellowstoneMapper {
    // Target is something like ProducedBlock
    type Target;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)>;
}

struct ExtractBlock(CommitmentConfig);

struct ExtractBlockMeta(CommitmentConfig);


pub fn create_multiplex<E>(
    grpc_sources: Vec<GrpcSourceConfig>,
    commitment_config: CommitmentConfig,
    extractor: E,
) -> impl Stream<Item = E::Target>
    where
        E: FromYellowstoneMapper,
{
    assert!(
        commitment_config == CommitmentConfig::confirmed()
            || commitment_config == CommitmentConfig::finalized(),
        "Only CONFIRMED and FINALIZED is supported");
    // note: PROCESSED blocks are not sequential in presense of forks; this will break the logic

    if grpc_sources.len() < 1 {
        panic!("Must have at least one source");
    }

    info!(
        "Starting multiplexer with {} sources: {}",
        grpc_sources.len(),
        grpc_sources
            .iter()
            .map(|source| source.label.clone())
            .join(", ")
    );

    let mut futures = futures::stream::SelectAll::new();

    for grpc_source in grpc_sources {
        futures.push(Box::pin(create_geyser_reconnecting_stream(
            grpc_source.clone(),
            commitment_config,
        )));
    }

    map_updates(futures, extractor)
}

fn map_updates<S, E>(geyser_stream: S, mapper: E) -> impl Stream<Item = E::Target>
    where
        S: Stream<Item = Option<SubscribeUpdate>>,
        E: FromYellowstoneMapper,
{
    let mut tip: Slot = 0;
    stream! {
        for await update in geyser_stream {
            if let Some(update) = update {
                if let Some((proposed_slot, block)) = mapper.map_yellowstone_update(update) {
                    if proposed_slot > tip {
                        tip = proposed_slot;
                        yield block;
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct GrpcSourceConfig {
    // symbolic name used in logs
    label: String,
    grpc_addr: String,
    grpc_x_token: Option<String>,
    tls_config: Option<ClientTlsConfig>,
}

impl GrpcSourceConfig {
    pub fn new(label: String, grpc_addr: String, grpc_x_token: Option<String>) -> Self {
        Self {
            label,
            grpc_addr,
            grpc_x_token,
            tls_config: None,
        }
    }
}

enum ConnectionState<S: Stream<Item = Result<SubscribeUpdate, Status>>> {
    NotConnected,
    Connecting(JoinHandle<GeyserGrpcClientResult<S>>),
    Ready(S),
    WaitReconnect,
}

// Connect to Geyser and return a generic stream of SubscribeUpdate
// note: stream never terminates
fn create_geyser_reconnecting_stream(
    grpc_source: GrpcSourceConfig,
    commitment_config: CommitmentConfig,
) -> impl Stream<Item = Option<SubscribeUpdate>> {
    let label = grpc_source.label.clone();

    // solana_sdk -> yellowstone
    let commitment_level = match commitment_config.commitment {
        solana_sdk::commitment_config::CommitmentLevel::Confirmed => yellowstone_grpc_proto::prelude::CommitmentLevel::Confirmed,
        solana_sdk::commitment_config::CommitmentLevel::Finalized => yellowstone_grpc_proto::prelude::CommitmentLevel::Finalized,
        _ => panic!("Only CONFIRMED and FINALIZED is supported/suggested"),
    };

    // NOT_CONNECTED; CONNECTING
    let mut state = ConnectionState::NotConnected;

    // in case of cancellation, we restart from here:
    // thus we want to keep the progression in a state object outside the stream! makro
    stream! {
        loop{
            let yield_value;
            (state, yield_value) = match state {
                ConnectionState::NotConnected => {

                    let connection_task = tokio::spawn({
                        let addr = grpc_source.grpc_addr.clone();
                        let token = grpc_source.grpc_x_token.clone();
                        let config = grpc_source.tls_config.clone();
                        // let (block_filter, blockmeta_filter) = blocks_filters.clone();
                        async move {

                            let connect_result = GeyserGrpcClient::connect_with_timeout(
                                addr, token, config,
                                Some(Duration::from_secs(2)), Some(Duration::from_secs(2)), false).await;
                            let mut client = connect_result?;

                            // TODO make filter configurable for caller
                            let mut blocks_subs = HashMap::new();
                            blocks_subs.insert(
                                "client".to_string(),
                                SubscribeRequestFilterBlocks {
                                    account_include: Default::default(),
                                    include_transactions: Some(true),
                                    include_accounts: Some(false),
                                    include_entries: Some(false),
                                },
                            );

                            let mut blocksmeta_subs = HashMap::new();
                            blocksmeta_subs.insert(
                                "client".to_string(),
                                SubscribeRequestFilterBlocksMeta {},
                            );

                            // Connected;
                            let subscribe_result = client
                                .subscribe_once(
                                    HashMap::new(),
                                    Default::default(),
                                    HashMap::new(),
                                    Default::default(),
                                    blocks_subs,
                                    blocksmeta_subs,
                                    Some(commitment_level),
                                    Default::default(),
                                    None,
                                ).await;

                            subscribe_result
                        }
                    });

                    (ConnectionState::Connecting(connection_task), None)
                }
                ConnectionState::Connecting(connection_task) => {
                    let subscribe_result = connection_task.await;

                     match subscribe_result {
                        Ok(Ok(subscribed_stream)) => (ConnectionState::Ready(subscribed_stream), None),
                        Ok(Err(geyser_error)) => {
                             // TODO identify non-recoverable errors and cancel stream
                            warn!("Subscribe failed on {} - retrying: {:?}", label, geyser_error);
                            (ConnectionState::WaitReconnect, None)
                        },
                        Err(geyser_grpc_task_error) => {
                            panic!("Task aborted - should not happen :{geyser_grpc_task_error}");
                        }
                    }

                }
                ConnectionState::Ready(mut geyser_stream) => {

                    //for await update_message in geyser_stream {
                        match geyser_stream.next().await {
                            Some(Ok(update_message)) => {
                                info!(">message on {}", label);
                                (ConnectionState::Ready(geyser_stream), Some(update_message))
                            }
                            Some(Err(tonic_status)) => {
                                // TODO identify non-recoverable errors and cancel stream
                                warn!("Receive error on {} - retrying: {:?}", label, tonic_status);
                                (ConnectionState::WaitReconnect, None)
                            }
                            None =>  {
                                //TODO should not arrive. Mean the stream close.
                                warn!("Geyzer stream close on {} - retrying", label);
                                (ConnectionState::WaitReconnect, None)
                            }
                        }
                    //} // -- production loop

                }
                ConnectionState::WaitReconnect => {
                    // TODO implement backoff
                    sleep(Duration::from_secs(1)).await;
                    (ConnectionState::NotConnected, None)
                }
            }; // -- match
            yield yield_value
        }

    } // -- stream!
}
