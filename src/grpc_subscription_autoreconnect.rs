use async_stream::stream;
use futures::{Stream, StreamExt};
use log::{debug, info, log, trace, warn, Level};
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::fmt::Display;
use std::pin::Pin;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientResult};
use yellowstone_grpc_proto::geyser::{SubscribeRequestFilterBlocks, SubscribeUpdate};
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterBlocksMeta;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::tonic::{async_trait, Status};

#[async_trait]
trait GrpcConnectionFactory: Clone {
    async fn connect_and_subscribe(
        &self,
    ) -> GeyserGrpcClientResult<Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>>>>>;
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

impl Display for GrpcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "grpc_addr {}",
            crate::obfuscate::url_obfuscate_api_token(&self.grpc_addr)
        )
    }
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
    GeyserSubscribeUpdate(Box<SubscribeUpdate>),
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
        solana_sdk::commitment_config::CommitmentLevel::Processed => {
            yellowstone_grpc_proto::prelude::CommitmentLevel::Processed
        }
        solana_sdk::commitment_config::CommitmentLevel::Confirmed => {
            yellowstone_grpc_proto::prelude::CommitmentLevel::Confirmed
        }
        solana_sdk::commitment_config::CommitmentLevel::Finalized => {
            yellowstone_grpc_proto::prelude::CommitmentLevel::Finalized
        }
        _ => {
            panic!(
                "unsupported commitment level {}",
                commitment_config.commitment
            )
        }
    };

    let mut state = ConnectionState::NotConnected(0);

    // in case of cancellation, we restart from here:
    // thus we want to keep the progression in a state object outside the stream! makro
    let the_stream = stream! {
        loop {
            let yield_value;

            (state, yield_value) = match state {

                ConnectionState::NotConnected(mut attempt) => {
                    attempt += 1;

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

                            debug!("Subscribe with blocks filter {:?} and blocksmeta filter {:?}", blocks_subs, blocksmeta_subs);

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
                            subscribe_result.map_err(|_| Status::unknown("unspecific subscribe timeout"))?
                        }
                    });

                    (ConnectionState::Connecting(attempt, connection_task), Message::Connecting(attempt))
                }

                ConnectionState::Connecting(attempt, connection_task) => {
                    let subscribe_result = connection_task.await;

                     match subscribe_result {
                        Ok(Ok(subscribed_stream)) => (ConnectionState::Ready(attempt, subscribed_stream), Message::Connecting(attempt)),
                        Ok(Err(geyser_error)) => {
                             // ATM we consider all errors recoverable
                            warn!("! subscribe failed on {} - retrying: {:?}", grpc_source, geyser_error);
                            (ConnectionState::WaitReconnect(attempt), Message::Connecting(attempt))
                        },
                        Err(geyser_grpc_task_error) => {
                            panic!("! task aborted - should not happen :{geyser_grpc_task_error}");
                        }
                    }

                }

                ConnectionState::Ready(attempt, mut geyser_stream) => {

                    match geyser_stream.next().await {
                        Some(Ok(update_message)) => {
                            trace!("> recv update message from {}", grpc_source);
                            (ConnectionState::Ready(attempt, geyser_stream), Message::GeyserSubscribeUpdate(Box::new(update_message)))
                        }
                        Some(Err(tonic_status)) => {
                            // ATM we consider all errors recoverable
                            debug!("! error on {} - retrying: {:?}", grpc_source, tonic_status);
                            (ConnectionState::WaitReconnect(attempt), Message::Connecting(attempt))
                        }
                        None =>  {
                            // should not arrive here, Mean the stream close.
                            panic!("geyser stream closed on {} - retrying", grpc_source);
                        }
                    }

                }

                ConnectionState::WaitReconnect(attempt) => {
                    let backoff_secs = 1.5_f32.powi(attempt as i32).min(15.0);
                    info!("! waiting {} seconds, then reconnect to {}", backoff_secs, grpc_source);
                    sleep(Duration::from_secs_f32(backoff_secs)).await;
                    (ConnectionState::NotConnected(attempt), Message::Connecting(attempt))
                }

            }; // -- match

            yield yield_value
        }

    }; // -- stream!

    the_stream
}
