use crate::grpc_subscription_autoreconnect_tasks::TheState::*;
use async_stream::stream;
use futures::channel::mpsc;
use futures::{Stream, StreamExt};
use log::{debug, error, info, log, trace, warn, Level};
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio::time::{sleep, timeout, Timeout};
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, GeyserGrpcClientResult};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeUpdate,
};
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterBlocksMeta;
use yellowstone_grpc_proto::tonic;
use yellowstone_grpc_proto::tonic::codegen::http::uri::InvalidUri;
use yellowstone_grpc_proto::tonic::metadata::errors::InvalidMetadataValue;
use yellowstone_grpc_proto::tonic::service::Interceptor;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::tonic::{Code, Status};
use crate::GrpcSourceConfig;

type Attempt = u32;

// wraps payload and status messages
// clone is required by broacast channel
#[derive(Clone)]
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
pub struct GeyserFilter(pub CommitmentConfig);

impl GeyserFilter {
    pub fn blocks_and_txs(&self) -> SubscribeRequest {
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

        SubscribeRequest {
            slots: HashMap::new(),
            accounts: Default::default(),
            transactions: HashMap::new(),
            entry: Default::default(),
            blocks: blocks_subs,
            blocks_meta: HashMap::new(),
            commitment: Some(map_commitment_level(self.0) as i32),
            accounts_data_slice: Default::default(),
            ping: None,
        }
    }

    pub fn blocks_meta(&self) -> SubscribeRequest {
        let mut blocksmeta_subs = HashMap::new();
        blocksmeta_subs.insert("client".to_string(), SubscribeRequestFilterBlocksMeta {});

        SubscribeRequest {
            slots: HashMap::new(),
            accounts: Default::default(),
            transactions: HashMap::new(),
            entry: Default::default(),
            blocks: HashMap::new(),
            blocks_meta: blocksmeta_subs,
            commitment: Some(map_commitment_level(self.0) as i32),
            accounts_data_slice: Default::default(),
            ping: None,
        }
    }
}

fn map_commitment_level(commitment_config: CommitmentConfig) -> CommitmentLevel {
    // solana_sdk -> yellowstone
    match commitment_config.commitment {
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
    }
}

enum TheState<S: Stream<Item = Result<SubscribeUpdate, Status>>, F: Interceptor> {
    NotConnected(Attempt),
    // Connected(Attempt, Box<Pin<GeyserGrpcClient<F>>>),
    Connected(Attempt, GeyserGrpcClient<F>),
    Ready(Attempt, S),
    // error states
    RecoverableConnectionError(Attempt),
    FatalError(Attempt),
    WaitReconnect(Attempt),
}

pub fn create_geyser_reconnecting_task(
    grpc_source: GrpcSourceConfig,
    subscribe_filter: SubscribeRequest,
) -> (JoinHandle<()>, Receiver<Message>) {
    let (sender, receiver_stream) = tokio::sync::broadcast::channel::<Message>(1000);

    let jh_geyser_task = tokio::spawn(async move {
        let mut state = NotConnected(0);

        loop {
            state = match state {
                NotConnected(mut attempt) => {
                    attempt += 1;

                    let addr = grpc_source.grpc_addr.clone();
                    let token = grpc_source.grpc_x_token.clone();
                    let config = grpc_source.tls_config.clone();
                    let connect_timeout = grpc_source.timeouts.as_ref().map(|t| t.connect_timeout);
                    let request_timeout = grpc_source.timeouts.as_ref().map(|t| t.request_timeout);
                    log!(
                        if attempt > 1 {
                            Level::Warn
                        } else {
                            Level::Debug
                        },
                        "Connecting attempt #{} to {}",
                        attempt,
                        addr
                    );
                    let connect_result = GeyserGrpcClient::connect_with_timeout(
                        addr,
                        token,
                        config,
                        connect_timeout,
                        request_timeout,
                        false,
                    )
                    .await;

                    match connect_result {
                        Ok(client) => Connected(attempt, client),
                        Err(GeyserGrpcClientError::InvalidUri(_)) => FatalError(attempt),
                        Err(GeyserGrpcClientError::MetadataValueError(_)) => FatalError(attempt),
                        Err(GeyserGrpcClientError::InvalidXTokenLength(_)) => FatalError(attempt),
                        Err(GeyserGrpcClientError::TonicError(tonic_error)) => {
                            warn!(
                                "! connect failed on {} - aborting: {:?}",
                                grpc_source, tonic_error
                            );
                            FatalError(attempt)
                        }
                        Err(GeyserGrpcClientError::TonicStatus(tonic_status)) => {
                            warn!(
                                "! connect failed on {} - retrying: {:?}",
                                grpc_source, tonic_status
                            );
                            RecoverableConnectionError(attempt)
                        }
                        Err(GeyserGrpcClientError::SubscribeSendError(send_error)) => {
                            warn!(
                                "! connect failed with send error on {} - retrying: {:?}",
                                grpc_source, send_error
                            );
                            RecoverableConnectionError(attempt)
                        }
                    }
                }
                Connected(attempt, mut client) => {
                    let subscribe_timeout =
                        grpc_source.timeouts.as_ref().map(|t| t.subscribe_timeout);
                    let subscribe_filter = subscribe_filter.clone();
                    debug!("Subscribe with filter {:?}", subscribe_filter);

                    let subscribe_result_timeout = timeout(
                        subscribe_timeout.unwrap_or(Duration::MAX),
                        client.subscribe_once2(subscribe_filter),
                    )
                    .await;

                    match subscribe_result_timeout {
                        Ok(subscribe_result) => {
                            match subscribe_result {
                                Ok(geyser_stream) => Ready(attempt, geyser_stream),
                                Err(GeyserGrpcClientError::TonicError(_)) => {
                                    warn!("! subscribe failed on {} - retrying", grpc_source);
                                    RecoverableConnectionError(attempt)
                                }
                                Err(GeyserGrpcClientError::TonicStatus(_)) => {
                                    warn!("! subscribe failed on {} - retrying", grpc_source);
                                    RecoverableConnectionError(attempt)
                                }
                                // non-recoverable
                                Err(unrecoverable_error) => {
                                    error!(
                                        "! subscribe to {} failed with unrecoverable error: {}",
                                        grpc_source, unrecoverable_error
                                    );
                                    FatalError(attempt)
                                }
                            }
                        }
                        Err(_elapsed) => {
                            warn!(
                                "! subscribe failed with timeout on {} - retrying",
                                grpc_source
                            );
                            RecoverableConnectionError(attempt)
                        }
                    }
                }
                RecoverableConnectionError(attempt) => {
                    let backoff_secs = 1.5_f32.powi(attempt as i32).min(15.0);
                    info!(
                        "! waiting {} seconds, then reconnect to {}",
                        backoff_secs, grpc_source
                    );
                    sleep(Duration::from_secs_f32(backoff_secs)).await;
                    NotConnected(attempt)
                }
                FatalError(_) => {
                    // TOOD what to do
                    panic!("Fatal error")
                }
                TheState::WaitReconnect(attempt) => {
                    let backoff_secs = 1.5_f32.powi(attempt as i32).min(15.0);
                    info!(
                        "! waiting {} seconds, then reconnect to {}",
                        backoff_secs, grpc_source
                    );
                    sleep(Duration::from_secs_f32(backoff_secs)).await;
                    TheState::NotConnected(attempt)
                }
                Ready(attempt, mut geyser_stream) => {
                    'recv_loop: loop {
                        match geyser_stream.next().await {
                            Some(Ok(update_message)) => {
                                trace!("> recv update message from {}", grpc_source);
                                match sender
                                    .send(Message::GeyserSubscribeUpdate(Box::new(update_message)))
                                {
                                    Ok(n_subscribers) => {
                                        trace!(
                                            "sent update message to {} subscribers (buffer={})",
                                            n_subscribers,
                                            sender.len()
                                        );
                                        continue 'recv_loop;
                                    }
                                    Err(SendError(_)) => {
                                        // note: error does not mean that future sends will also fail!
                                        trace!("no subscribers for update message");
                                        continue 'recv_loop;
                                    }
                                };
                            }
                            Some(Err(tonic_status)) => {
                                // all tonic errors are recoverable
                                warn!("! error on {} - retrying: {:?}", grpc_source, tonic_status);
                                break 'recv_loop TheState::WaitReconnect(attempt);
                            }
                            None => {
                                warn!("geyser stream closed on {} - retrying", grpc_source);
                                break 'recv_loop TheState::WaitReconnect(attempt);
                            }
                        }
                    } // -- end loop
                }
            }
        }
    });

    (jh_geyser_task, receiver_stream)
}

#[cfg(test)]
mod tests {
    use crate::GrpcConnectionTimeouts;
    use super::*;

    #[tokio::test]
    async fn test_debug_no_secrets() {
        let timeout_config = GrpcConnectionTimeouts {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(2),
            subscribe_timeout: Duration::from_secs(3),
        };
        assert_eq!(
            format!(
                "{:?}",
                GrpcSourceConfig::new(
                    "http://localhost:1234".to_string(),
                    Some("my-secret".to_string()),
                    None,
                    timeout_config
                )
            ),
            "grpc_addr http://localhost:1234"
        );
    }

    #[tokio::test]
    async fn test_display_no_secrets() {
        let timeout_config = GrpcConnectionTimeouts {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(2),
            subscribe_timeout: Duration::from_secs(3),
        };
        assert_eq!(
            format!(
                "{}",
                GrpcSourceConfig::new(
                    "http://localhost:1234".to_string(),
                    Some("my-secret".to_string()),
                    None,
                    timeout_config
                )
            ),
            "grpc_addr http://localhost:1234"
        );
    }
}
