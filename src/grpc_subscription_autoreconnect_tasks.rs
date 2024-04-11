use crate::{Attempt, GrpcSourceConfig, Message};
use futures::{Stream, StreamExt};
use log::{debug, error, info, log, trace, warn, Level};
use std::time::Duration;
use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::sync::mpsc::Receiver;
use tokio::task::AbortHandle;
use tokio::time::{sleep, timeout, Instant};
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError};
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeUpdate};
use yellowstone_grpc_proto::tonic::service::Interceptor;
use yellowstone_grpc_proto::tonic::Status;
use crate::yellowstone_grpc_util::{connect_with_timeout_with_buffers, GeyserGrpcClientBufferConfig};

enum ConnectionState<S: Stream<Item = Result<SubscribeUpdate, Status>>, F: Interceptor> {
    NotConnected(Attempt),
    // connected but not subscribed
    Connecting(Attempt, GeyserGrpcClient<F>),
    Ready(S),
    // error states
    RecoverableConnectionError(Attempt),
    // non-recoverable error
    FatalError(Attempt, FatalErrorReason),
    WaitReconnect(Attempt),
}

enum FatalErrorReason {
    DownstreamChannelClosed,
    ConfigurationError,
    NetworkError,
    SubscribeError,
}

pub fn create_geyser_autoconnection_task(
    grpc_source: GrpcSourceConfig,
    subscribe_filter: SubscribeRequest,
) -> (AbortHandle, Receiver<Message>) {
    let (sender, receiver_channel) = tokio::sync::mpsc::channel::<Message>(1);

    let abort_handle =
        create_geyser_autoconnection_task_with_mpsc(grpc_source, subscribe_filter, sender);

    (abort_handle, receiver_channel)
}

/// connect to grpc source performing autoconnect if required,
/// returns mpsc channel; task will abort on fatal error
/// will shut down when receiver is dropped
pub fn create_geyser_autoconnection_task_with_mpsc(
    grpc_source: GrpcSourceConfig,
    subscribe_filter: SubscribeRequest,
    mpsc_downstream: tokio::sync::mpsc::Sender<Message>,
) -> AbortHandle {
    // read this for argument: http://www.randomhacks.net/2019/03/08/should-rust-channels-panic-on-send/

    // task will be aborted when downstream receiver gets dropped
    let jh_geyser_task = tokio::spawn(async move {
        let mut state = ConnectionState::NotConnected(1);
        let mut messages_forwarded = 0;

        loop {
            state = match state {
                ConnectionState::NotConnected(attempt) => {
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
                        "Connecting attempt {} to {}",
                        attempt,
                        addr
                    );

                    let connect_result = connect_with_timeout_with_buffers(
                        addr,
                        token,
                        config,
                        connect_timeout,
                        request_timeout,
                        GeyserGrpcClientBufferConfig::optimize_for_subscription(&subscribe_filter),
                    )
                    .await;

                    match connect_result {
                        Ok(client) => ConnectionState::Connecting(attempt, client),
                        Err(GeyserGrpcClientError::InvalidUri(_)) => ConnectionState::FatalError(
                            attempt + 1,
                            FatalErrorReason::ConfigurationError,
                        ),
                        Err(GeyserGrpcClientError::MetadataValueError(_)) => {
                            ConnectionState::FatalError(
                                attempt + 1,
                                FatalErrorReason::ConfigurationError,
                            )
                        }
                        Err(GeyserGrpcClientError::InvalidXTokenLength(_)) => {
                            ConnectionState::FatalError(
                                attempt + 1,
                                FatalErrorReason::ConfigurationError,
                            )
                        }
                        Err(GeyserGrpcClientError::TonicError(tonic_error)) => {
                            warn!(
                                "connect failed on {} - aborting: {:?}",
                                grpc_source, tonic_error
                            );
                            ConnectionState::FatalError(attempt + 1, FatalErrorReason::NetworkError)
                        }
                        Err(GeyserGrpcClientError::TonicStatus(tonic_status)) => {
                            warn!(
                                "connect failed on {} - retrying: {:?}",
                                grpc_source, tonic_status
                            );
                            ConnectionState::RecoverableConnectionError(attempt + 1)
                        }
                        Err(GeyserGrpcClientError::SubscribeSendError(send_error)) => {
                            warn!(
                                "connect failed with send error on {} - retrying: {:?}",
                                grpc_source, send_error
                            );
                            ConnectionState::RecoverableConnectionError(attempt + 1)
                        }
                    }
                }
                ConnectionState::Connecting(attempt, mut client) => {
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
                                Ok(geyser_stream) => {
                                    if attempt > 1 {
                                        debug!(
                                            "subscribed to {} after {} failed attempts",
                                            grpc_source, attempt
                                        );
                                    }
                                    ConnectionState::Ready(geyser_stream)
                                }
                                Err(GeyserGrpcClientError::TonicError(_)) => {
                                    warn!(
                                        "subscribe failed on {} after {} attempts - retrying",
                                        grpc_source, attempt
                                    );
                                    ConnectionState::RecoverableConnectionError(attempt + 1)
                                }
                                Err(GeyserGrpcClientError::TonicStatus(_)) => {
                                    warn!(
                                        "subscribe failed on {} after {} attempts - retrying",
                                        grpc_source, attempt
                                    );
                                    ConnectionState::RecoverableConnectionError(attempt + 1)
                                }
                                // non-recoverable
                                Err(unrecoverable_error) => {
                                    error!(
                                        "subscribe to {} failed with unrecoverable error: {}",
                                        grpc_source, unrecoverable_error
                                    );
                                    ConnectionState::FatalError(
                                        attempt + 1,
                                        FatalErrorReason::SubscribeError,
                                    )
                                }
                            }
                        }
                        Err(_elapsed) => {
                            warn!(
                                "subscribe failed with timeout on {} - retrying",
                                grpc_source
                            );
                            ConnectionState::RecoverableConnectionError(attempt + 1)
                        }
                    }
                }
                ConnectionState::RecoverableConnectionError(attempt) => {
                    let backoff_secs = 1.5_f32.powi(attempt as i32).min(15.0);
                    info!(
                        "waiting {} seconds, then reconnect to {}",
                        backoff_secs, grpc_source
                    );
                    sleep(Duration::from_secs_f32(backoff_secs)).await;
                    ConnectionState::NotConnected(attempt)
                }
                ConnectionState::FatalError(_attempt, reason) => match reason {
                    FatalErrorReason::DownstreamChannelClosed => {
                        warn!("downstream closed - aborting");
                        return;
                    }
                    FatalErrorReason::ConfigurationError => {
                        warn!("fatal configuration error - aborting");
                        return;
                    }
                    FatalErrorReason::NetworkError => {
                        warn!("fatal network error - aborting");
                        return;
                    }
                    FatalErrorReason::SubscribeError => {
                        warn!("fatal grpc subscribe error - aborting");
                        return;
                    }
                },
                ConnectionState::WaitReconnect(attempt) => {
                    let backoff_secs = 1.5_f32.powi(attempt as i32).min(15.0);
                    info!(
                        "waiting {} seconds, then reconnect to {}",
                        backoff_secs, grpc_source
                    );
                    sleep(Duration::from_secs_f32(backoff_secs)).await;
                    ConnectionState::NotConnected(attempt)
                }
                ConnectionState::Ready(mut geyser_stream) => {
                    let receive_timeout = grpc_source.timeouts.as_ref().map(|t| t.receive_timeout);
                    'recv_loop: loop {
                        match timeout(
                            receive_timeout.unwrap_or(Duration::MAX),
                            geyser_stream.next(),
                        )
                        .await
                        {
                            Ok(Some(Ok(update_message))) => {
                                trace!("> recv update message from {}", grpc_source);
                                // note: first send never blocks as the mpsc channel has capacity 1
                                let warning_threshold = if messages_forwarded == 1 {
                                    Duration::from_millis(3000)
                                } else {
                                    Duration::from_millis(500)
                                };
                                let started_at = Instant::now();
                                match mpsc_downstream
                                    .send_timeout(
                                        Message::GeyserSubscribeUpdate(Box::new(update_message)),
                                        warning_threshold,
                                    )
                                    .await
                                {
                                    Ok(()) => {
                                        messages_forwarded += 1;
                                        if messages_forwarded == 1 {
                                            // note: first send never blocks - do not print time as this is a lie
                                            trace!("queued first update message");
                                        } else {
                                            trace!(
                                                "queued update message {} in {:.02}ms",
                                                messages_forwarded,
                                                started_at.elapsed().as_secs_f32() * 1000.0
                                            );
                                        }
                                        continue 'recv_loop;
                                    }
                                    Err(SendTimeoutError::Timeout(the_message)) => {
                                        warn!("downstream receiver did not pick up message for {}ms - keep waiting", warning_threshold.as_millis());

                                        match mpsc_downstream.send(the_message).await {
                                            Ok(()) => {
                                                messages_forwarded += 1;
                                                trace!(
                                                    "queued delayed update message {} in {:.02}ms",
                                                    messages_forwarded,
                                                    started_at.elapsed().as_secs_f32() * 1000.0
                                                );
                                            }
                                            Err(_send_error) => {
                                                warn!("downstream receiver closed, message is lost - aborting");
                                                break 'recv_loop ConnectionState::FatalError(
                                                    0,
                                                    FatalErrorReason::DownstreamChannelClosed,
                                                );
                                            }
                                        }
                                    }
                                    Err(SendTimeoutError::Closed(_)) => {
                                        warn!("downstream receiver closed - aborting");
                                        break 'recv_loop ConnectionState::FatalError(
                                            0,
                                            FatalErrorReason::DownstreamChannelClosed,
                                        );
                                    }
                                }
                            }
                            Ok(Some(Err(tonic_status))) => {
                                // all tonic errors are recoverable
                                warn!("error on {} - retrying: {:?}", grpc_source, tonic_status);
                                break 'recv_loop ConnectionState::WaitReconnect(1);
                            }
                            Ok(None) => {
                                warn!("geyser stream closed on {} - retrying", grpc_source);
                                break 'recv_loop ConnectionState::WaitReconnect(1);
                            }
                            Err(_elapsed) => {
                                warn!("timeout on {} - retrying", grpc_source);
                                break 'recv_loop ConnectionState::WaitReconnect(1);
                            }
                        } // -- END match
                    } // -- END receive loop
                }
            } // -- END match
        } // -- endless state loop
    });

    jh_geyser_task.abort_handle()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GrpcConnectionTimeouts;

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
            receive_timeout: Duration::from_secs(3),
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
