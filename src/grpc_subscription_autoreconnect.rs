use async_stream::stream;
use futures::{Stream, StreamExt};
use log::{debug, info, log, trace, warn, Level};
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio::time::{sleep, sleep_until, timeout};
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientResult};
use yellowstone_grpc_proto::geyser::{SubscribeRequestFilterBlocks, SubscribeUpdate};
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterBlocksMeta;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::tonic::{async_trait, Status};

#[async_trait]
trait GrpcConnectionFactory: Clone {
    async fn connect_and_subscribe(&self)
        -> GeyserGrpcClientResult<Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>>>>>;
}

#[derive(Clone, Debug)]
pub struct GrpcConnectionTimeouts {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub subscribe_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct GrpcSourceConfig {
    grpc_addr: String,
    grpc_x_token: Option<String>,
    tls_config: Option<ClientTlsConfig>,
    timeouts: Option<GrpcConnectionTimeouts>,
}

impl GrpcSourceConfig {
    /// Create a grpc source without tls and timeouts
    pub fn new_simple(grpc_addr: String) -> Self {
        Self {
            grpc_addr,
            grpc_x_token: None,
            tls_config: None,
            timeouts: None,
        }
    }
    pub fn new(
        grpc_addr: String,
        grpc_x_token: Option<String>,
        tls_config: Option<ClientTlsConfig>,
        timeouts: GrpcConnectionTimeouts,
    ) -> Self {
        Self {
            grpc_addr,
            grpc_x_token,
            tls_config,
            timeouts: Some(timeouts),
        }
    }
}

type Attempt = u32;

// wraps payload and status messages
pub enum Message {
    GeyserSubscribeUpdate(SubscribeUpdate),
    // connect (attempt=1) or reconnect(attempt=2..)
    Connecting(Attempt),
}

enum ConnectionState<S: Stream<Item = Result<SubscribeUpdate, Status>>> {
    NotConnected(Attempt),
    Connecting(Attempt, JoinHandle<GeyserGrpcClientResult<S>>),
    Ready(Attempt, S),
    WaitReconnect(Attempt),
}

#[derive(Clone)]
pub enum GeyserFilter {
    Blocks(SubscribeRequestFilterBlocks),
    BlocksMeta(SubscribeRequestFilterBlocksMeta),
}

impl GeyserFilter {
    pub fn blocks_and_txs() -> Self {
        Self::Blocks(SubscribeRequestFilterBlocks {
            account_include: Default::default(),
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(false),
        })
    }
    pub fn blocks_filter(filter: SubscribeRequestFilterBlocks) -> Self {
        Self::Blocks(filter)
    }
    // no parameters available
    pub fn blocks_meta() -> Self {
        Self::BlocksMeta(SubscribeRequestFilterBlocksMeta {})
    }
}


// Take geyser filter, connect to Geyser and return a generic stream of SubscribeUpdate
// note: stream never terminates
pub fn create_geyser_reconnecting_stream(
    grpc_source: GrpcSourceConfig,
    filter: GeyserFilter,
    commitment_config: CommitmentConfig,
) -> impl Stream<Item = Message> {

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

    let mut state = ConnectionState::NotConnected(0);

    // in case of cancellation, we restart from here:
    // thus we want to keep the progression in a state object outside the stream! makro
    let the_stream = stream! {
        loop {
            let yield_value;

            (state, yield_value) = match state {

                ConnectionState::NotConnected(mut attempt) => {
                    attempt = attempt + 1;

                    let connection_task = tokio::spawn({
                        let addr = grpc_source.grpc_addr.clone();
                        let token = grpc_source.grpc_x_token.clone();
                        let config = grpc_source.tls_config.clone();
                        let connect_timeout = grpc_source.timeouts.as_ref().map(|t| t.connect_timeout);
                        let request_timeout = grpc_source.timeouts.as_ref().map(|t| t.request_timeout);
                        let subscribe_timeout = grpc_source.timeouts.as_ref().map(|t| t.subscribe_timeout);
                        let filter = filter.clone();
                        log!(if attempt > 1 { Level::Warn } else { Level::Debug }, "Connecting attempt #{} to {}", attempt, addr);
                        async move {

                            let connect_result = GeyserGrpcClient::connect_with_timeout(
                                    addr, token, config,
                                    connect_timeout,
                                    request_timeout,
                                false)
                                .await;
                            let mut client = connect_result?;

                            let mut blocks_subs = HashMap::new();
                            let mut blocksmeta_subs = HashMap::new();
                            match filter {
                                GeyserFilter::Blocks(filter) => {
                                    blocks_subs.insert(
                                        "client".to_string(),
                                        filter,
                                    );
                                }
                                GeyserFilter::BlocksMeta(filter) => {
                                    blocksmeta_subs.insert(
                                        "client".to_string(),
                                        filter,
                                    );
                                }
                            }

                            let subscribe_result = timeout(subscribe_timeout.unwrap_or(Duration::MAX),
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
                                    ))
                            .await;

                            // maybe not optimal
                            subscribe_result.map_err(|_| Status::unknown("subscribe timeout"))?
                        }
                    });

                    (ConnectionState::Connecting(attempt, connection_task), Message::Connecting(attempt))
                }

                ConnectionState::Connecting(attempt, connection_task) => {
                    let subscribe_result = connection_task.await;

                     match subscribe_result {
                        Ok(Ok(subscribed_stream)) => (ConnectionState::Ready(attempt, subscribed_stream), Message::Connecting(attempt)),
                        Ok(Err(geyser_error)) => {
                             // TODO identify non-recoverable errors and cancel stream
                            warn!("Subscribe failed - retrying: {:?}", geyser_error);
                            (ConnectionState::WaitReconnect(attempt), Message::Connecting(attempt))
                        },
                        Err(geyser_grpc_task_error) => {
                            panic!("Task aborted - should not happen :{geyser_grpc_task_error}");
                        }
                    }

                }

                ConnectionState::Ready(attempt, mut geyser_stream) => {

                    match geyser_stream.next().await {
                        Some(Ok(update_message)) => {
                            trace!("> recv update message");
                            (ConnectionState::Ready(attempt, geyser_stream), Message::GeyserSubscribeUpdate(update_message))
                        }
                        Some(Err(tonic_status)) => {
                            // TODO identify non-recoverable errors and cancel stream
                            debug!("! error - retrying: {:?}", tonic_status);
                            (ConnectionState::WaitReconnect(attempt), Message::Connecting(attempt))
                        }
                        None =>  {
                            //TODO should not arrive. Mean the stream close.
                            warn!("! geyser stream closed - retrying");
                            (ConnectionState::WaitReconnect(attempt), Message::Connecting(attempt))
                        }
                    }

                }

                ConnectionState::WaitReconnect(attempt) => {
                    let backoff_secs = 1.5_f32.powi(attempt as i32).min(15.0);
                    info!("Waiting {} seconds, then reconnect", backoff_secs);
                    sleep(Duration::from_secs_f32(backoff_secs)).await;
                    (ConnectionState::NotConnected(attempt), Message::Connecting(attempt))
                }

            }; // -- match

            yield yield_value
        }

    }; // -- stream!

    the_stream
}
