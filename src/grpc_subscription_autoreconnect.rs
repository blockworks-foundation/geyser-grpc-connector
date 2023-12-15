use async_stream::stream;
use futures::{Stream, StreamExt};
use log::{debug, info, trace, warn};
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientResult};
use yellowstone_grpc_proto::geyser::{SubscribeRequestFilterBlocks, SubscribeUpdate};
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterBlocksMeta;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::tonic::{async_trait, Status};

#[async_trait]
trait GrpcConnectionFactory: Clone {
    // async fn connect() -> GeyserGrpcClientResult<impl Stream<Item=Result<SubscribeUpdate, Status>>+Sized>;
    async fn connect_and_subscribe(
        &self,
    ) -> GeyserGrpcClientResult<Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>>>>>;
}

#[derive(Clone, Debug)]
pub struct GrpcSourceConfig {
    // symbolic name used in logs
    pub label: String,
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

// Takes geyser filter for geyser, connect to Geyser and return a generic stream of SubscribeUpdate
// note: stream never terminates
pub fn create_geyser_reconnecting_stream(
    grpc_source: GrpcSourceConfig,
    commitment_config: CommitmentConfig,
    // TODO do we want Option<SubscribeUpdate>
) -> impl Stream<Item = Option<SubscribeUpdate>> {
    let label = grpc_source.label.clone();

    // solana_sdk -> yellowstone
    let commitment_level = match commitment_config.commitment {
        solana_sdk::commitment_config::CommitmentLevel::Confirmed => {
            yellowstone_grpc_proto::prelude::CommitmentLevel::Confirmed
        }
        solana_sdk::commitment_config::CommitmentLevel::Finalized => {
            yellowstone_grpc_proto::prelude::CommitmentLevel::Finalized
        }
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


                            client
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
                                ).await
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

                    match geyser_stream.next().await {
                        Some(Ok(update_message)) => {
                            trace!("> update message on {}", label);
                            (ConnectionState::Ready(geyser_stream), Some(update_message))
                        }
                        Some(Err(tonic_status)) => {
                            // TODO identify non-recoverable errors and cancel stream
                            debug!("! error on {} - retrying: {:?}", label, tonic_status);
                            (ConnectionState::WaitReconnect, None)
                        }
                        None =>  {
                            //TODO should not arrive. Mean the stream close.
                            warn!("! geyser stream closed on {} - retrying", label);
                            (ConnectionState::WaitReconnect, None)
                        }
                    }

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
