/// NOT MAINTAINED - please use the `grpc_subscription_autoreconnect_streams` module instead
use std::time::Duration;

use async_stream::stream;
use futures::{Stream, StreamExt};
use log::{debug, info, log, trace, warn, Level};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use yellowstone_grpc_client::GeyserGrpcClientResult;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeUpdate};
use yellowstone_grpc_proto::tonic::Status;

use crate::yellowstone_grpc_util::{
    connect_with_timeout_with_buffers, GeyserGrpcClientBufferConfig,
};
use crate::{Attempt, GrpcSourceConfig, Message};

enum ConnectionState<S: Stream<Item = Result<SubscribeUpdate, Status>>> {
    NotConnected(Attempt),
    Connecting(Attempt, JoinHandle<GeyserGrpcClientResult<S>>),
    Ready(S),
    WaitReconnect(Attempt),
}

// Take geyser filter, connect to Geyser and return a generic stream of SubscribeUpdate
// note: stream never terminates
pub fn create_geyser_reconnecting_stream(
    grpc_source: GrpcSourceConfig,
    subscribe_filter: SubscribeRequest,
) -> impl Stream<Item = Message> {
    let mut state = ConnectionState::NotConnected(1);

    // in case of cancellation, we restart from here:
    // thus we want to keep the progression in a state object outside the stream! macro
    let the_stream = stream! {
        loop {
            let yield_value;

            (state, yield_value) = match state {

                ConnectionState::NotConnected(attempt) => {

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

                            let connect_result = connect_with_timeout_with_buffers(
                                addr,
                                token,
                                config,
                                connect_timeout,
                                request_timeout,
                                GeyserGrpcClientBufferConfig::optimize_for_subscription(&subscribe_filter),
                                None,
                            )
                            .await;

                            let mut client = connect_result.unwrap(); // FIXME how to handle this?
                            debug!("Subscribe with filter {:?}", subscribe_filter);

                            let subscribe_result = timeout(subscribe_timeout.unwrap_or(Duration::MAX),
                                client.subscribe_once(subscribe_filter)).await;

                            // maybe not optimal
                            subscribe_result.map_err(|_| Status::unknown("unspecific subscribe timeout"))?
                        }
                    });

                    (ConnectionState::Connecting(attempt + 1, connection_task), Message::Connecting(attempt))
                }

                ConnectionState::Connecting(attempt, connection_task) => {
                    let subscribe_result = connection_task.await;

                     match subscribe_result {
                        Ok(Ok(subscribed_stream)) => (ConnectionState::Ready(subscribed_stream), Message::Connecting(attempt)),
                        Ok(Err(geyser_error)) => {
                             // ATM we consider all errors recoverable
                            warn!("subscribe failed on {} - retrying: {:#}", grpc_source, geyser_error);
                            (ConnectionState::WaitReconnect(attempt + 1), Message::Connecting(attempt))
                        },
                        Err(geyser_grpc_task_error) => {
                            warn!("connection task aborted on {} - retrying: {:#}", grpc_source, geyser_grpc_task_error);
                            (ConnectionState::WaitReconnect(attempt + 1), Message::Connecting(attempt))
                        }
                    }

                }

                ConnectionState::Ready(mut geyser_stream) => {
                    let receive_timeout = grpc_source.timeouts.as_ref().map(|t| t.receive_timeout);
                    match timeout(receive_timeout.unwrap_or(Duration::MAX), geyser_stream.next()).await {
                        Ok(Some(Ok(update_message))) => {
                            trace!("> recv update message from {}", grpc_source);
                            (ConnectionState::Ready(geyser_stream), Message::GeyserSubscribeUpdate(Box::new(update_message)))
                        }
                        Ok(Some(Err(tonic_status))) => {
                            // ATM we consider all errors recoverable
                            warn!("error on {} - retrying: {:#}", grpc_source, tonic_status);
                            (ConnectionState::WaitReconnect(1), Message::Connecting(1))
                        }
                        Ok(None) =>  {
                            // should not arrive here, Mean the stream close.
                            warn!("geyser stream closed on {} - retrying", grpc_source);
                            (ConnectionState::WaitReconnect(1), Message::Connecting(1))
                        }
                        Err(_elapsed) => {
                            // timeout
                            warn!("geyser stream timeout on {} - retrying", grpc_source);
                            (ConnectionState::WaitReconnect(1), Message::Connecting(1))
                        }
                    }

                }

                ConnectionState::WaitReconnect(attempt) => {
                    let backoff_secs = 1.5_f32.powi(attempt as i32).min(15.0);
                    info!("waiting {} seconds, then reconnect to {}", backoff_secs, grpc_source);
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
            receive_timeout: Duration::from_secs(3),
        };
        assert_eq!(
            format!(
                "{:?}",
                GrpcSourceConfig::new(
                    "http://localhost:1234".to_string(),
                    Some("my-secret".to_string()),
                    None,
                    timeout_config,
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
            receive_timeout: Duration::from_secs(3),
        };
        assert_eq!(
            format!(
                "{}",
                GrpcSourceConfig::new(
                    "http://localhost:1234".to_string(),
                    Some("my-secret".to_string()),
                    None,
                    timeout_config,
                )
            ),
            "grpc_addr http://localhost:1234"
        );
    }
}
