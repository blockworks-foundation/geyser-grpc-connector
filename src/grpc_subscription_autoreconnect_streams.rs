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

// Take geyser filter, connect to Geyser and return a generic stream of SubscribeUpdate
// note: stream never terminates
pub fn create_geyser_reconnecting_stream(
    grpc_source: GrpcSourceConfig,
    subscribe_filter: SubscribeRequest,
) -> impl Stream<Item = Message> {
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
                        let subscribe_filter = subscribe_filter.clone();
                        log!(if attempt > 1 { Level::Warn } else { Level::Debug }, "Connecting attempt #{} to {}", attempt, addr);
                        async move {

                            let connect_result = GeyserGrpcClient::connect_with_timeout(
                                    addr, token, config,
                                    connect_timeout,
                                    request_timeout,
                                false)
                                .await;
                            let mut client = connect_result?;


                            debug!("Subscribe with filter {:?}", subscribe_filter);

                            let subscribe_result = timeout(subscribe_timeout.unwrap_or(Duration::MAX),
                                client
                                    .subscribe_once2(subscribe_filter))
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
                            warn!("! error on {} - retrying: {:?}", grpc_source, tonic_status);
                            (ConnectionState::WaitReconnect(attempt), Message::Connecting(attempt))
                        }
                        None =>  {
                            // should not arrive here, Mean the stream close.
                            warn!("geyser stream closed on {} - retrying", grpc_source);
                            (ConnectionState::WaitReconnect(attempt), Message::Connecting(attempt))
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
