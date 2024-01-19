use futures::{Stream, StreamExt};
use log::{debug, error, info, log, trace, warn, Level};
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use anyhow::bail;
use tokio::sync::mpsc::error::{SendError, SendTimeoutError};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio::time::{Instant, sleep, timeout, Timeout};
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

#[derive(Debug, Clone)]
pub enum AutoconnectionError {
    AbortedFatalError,
}

enum ConnectionState<S: Stream<Item = Result<SubscribeUpdate, Status>>> {
    NotConnected(Attempt),
    Connecting(Attempt, JoinHandle<GeyserGrpcClientResult<S>>),
    Ready(Attempt, S),
    WaitReconnect(Attempt),
}

enum FatalErrorReason {
    DownstreamChannelClosed,
    ConfigurationError,
    NetworkError,
    SubscribeError,
    // everything else
    Misc,
}

enum State<S: Stream<Item = Result<SubscribeUpdate, Status>>, F: Interceptor> {
    NotConnected(Attempt),
    Connected(Attempt, GeyserGrpcClient<F>),
    Ready(Attempt, S),
    // error states
    RecoverableConnectionError(Attempt),
    // non-recoverable error
    FatalError(Attempt, FatalErrorReason),
    WaitReconnect(Attempt),
}

/// connect to grpc source performing autoconect if required,
/// returns mpsc channel; task will abort on fatal error
///
/// implementation hints:
/// * no panic/unwrap
/// * do not use "?"
/// * do not "return" unless you really want to abort the task
pub fn create_geyser_autoconnection_task(
    grpc_source: GrpcSourceConfig,
    subscribe_filter: SubscribeRequest,
) -> (JoinHandle<Result<(), AutoconnectionError>>, Receiver<Message>) {
    // read this for argument: http://www.randomhacks.net/2019/03/08/should-rust-channels-panic-on-send/
    let (sender, receiver_stream) = tokio::sync::mpsc::channel::<Message>(1);

    let jh_geyser_task = tokio::spawn(async move {
        let mut state = State::NotConnected(0);
        let mut messages_forwarded = 0;

        loop {
            state = match state {
                State::NotConnected(mut attempt) => {
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
                        Ok(client) => State::Connected(attempt, client),
                        Err(GeyserGrpcClientError::InvalidUri(_)) => State::FatalError(attempt, FatalErrorReason::ConfigurationError),
                        Err(GeyserGrpcClientError::MetadataValueError(_)) => State::FatalError(attempt, FatalErrorReason::ConfigurationError),
                        Err(GeyserGrpcClientError::InvalidXTokenLength(_)) => State::FatalError(attempt, FatalErrorReason::ConfigurationError),
                        Err(GeyserGrpcClientError::TonicError(tonic_error)) => {
                            warn!(
                                "! connect failed on {} - aborting: {:?}",
                                grpc_source, tonic_error
                            );
                            State::FatalError(attempt, FatalErrorReason::NetworkError)
                        }
                        Err(GeyserGrpcClientError::TonicStatus(tonic_status)) => {
                            warn!(
                                "! connect failed on {} - retrying: {:?}",
                                grpc_source, tonic_status
                            );
                            State::RecoverableConnectionError(attempt)
                        }
                        Err(GeyserGrpcClientError::SubscribeSendError(send_error)) => {
                            warn!(
                                "! connect failed with send error on {} - retrying: {:?}",
                                grpc_source, send_error
                            );
                            State::RecoverableConnectionError(attempt)
                        }
                    }
                }
                State::Connected(attempt, mut client) => {
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
                                Ok(geyser_stream) => State::Ready(attempt, geyser_stream),
                                Err(GeyserGrpcClientError::TonicError(_)) => {
                                    warn!("! subscribe failed on {} - retrying", grpc_source);
                                    State::RecoverableConnectionError(attempt)
                                }
                                Err(GeyserGrpcClientError::TonicStatus(_)) => {
                                    warn!("! subscribe failed on {} - retrying", grpc_source);
                                    State::RecoverableConnectionError(attempt)
                                }
                                // non-recoverable
                                Err(unrecoverable_error) => {
                                    error!(
                                        "! subscribe to {} failed with unrecoverable error: {}",
                                        grpc_source, unrecoverable_error
                                    );
                                    State::FatalError(attempt, FatalErrorReason::SubscribeError)
                                }
                            }
                        }
                        Err(_elapsed) => {
                            warn!(
                                "! subscribe failed with timeout on {} - retrying",
                                grpc_source
                            );
                            State::RecoverableConnectionError(attempt)
                        }
                    }
                }
                State::RecoverableConnectionError(attempt) => {
                    let backoff_secs = 1.5_f32.powi(attempt as i32).min(15.0);
                    info!(
                        "! waiting {} seconds, then reconnect to {}",
                        backoff_secs, grpc_source
                    );
                    sleep(Duration::from_secs_f32(backoff_secs)).await;
                    State::NotConnected(attempt)
                }
                State::FatalError(_attempt, reason) => {
                    match reason {
                        FatalErrorReason::DownstreamChannelClosed => {
                            warn!("! downstream closed - aborting");
                            return Err(AutoconnectionError::AbortedFatalError);
                        }
                        FatalErrorReason::ConfigurationError => {
                            warn!("! fatal configuration error - aborting");
                            return Err(AutoconnectionError::AbortedFatalError);
                        }
                        FatalErrorReason::NetworkError => {
                            warn!("! fatal network error - aborting");
                            return Err(AutoconnectionError::AbortedFatalError);
                        }
                        FatalErrorReason::SubscribeError => {
                            warn!("! fatal grpc subscribe error - aborting");
                            return Err(AutoconnectionError::AbortedFatalError);
                        }
                        FatalErrorReason::Misc => {
                            error!("! fatal misc error grpc connection - aborting");
                            return Err(AutoconnectionError::AbortedFatalError);
                        }
                    }
                }
                State::WaitReconnect(attempt) => {
                    let backoff_secs = 1.5_f32.powi(attempt as i32).min(15.0);
                    info!(
                        "! waiting {} seconds, then reconnect to {}",
                        backoff_secs, grpc_source
                    );
                    sleep(Duration::from_secs_f32(backoff_secs)).await;
                    State::NotConnected(attempt)
                }
                State::Ready(attempt, mut geyser_stream) => {
                    'recv_loop: loop {
                        match geyser_stream.next().await {
                            Some(Ok(update_message)) => {
                                trace!("> recv update message from {}", grpc_source);
                                // TODO consider extract this
                                // backpressure - should'n we block here?
                                // TODO extract timeout param; TODO respect startup
                                // emit warning if message not received
                                // note: first send never blocks
                                let warning_threshold = if messages_forwarded == 1 { Duration::from_millis(3000) } else { Duration::from_millis(500) };
                                let started_at = Instant::now();
                                match sender.send_timeout(Message::GeyserSubscribeUpdate(Box::new(update_message)), warning_threshold).await {
                                    Ok(()) => {
                                        messages_forwarded += 1;
                                        if messages_forwarded == 1 {
                                            // note: first send never blocks - do not print time as this is a lie
                                            trace!("queued first update message");
                                        } else {
                                            trace!("queued update message {} in {:.02}ms",
                                                messages_forwarded, started_at.elapsed().as_secs_f32() * 1000.0);
                                        }
                                        continue 'recv_loop;
                                    }
                                    Err(SendTimeoutError::Timeout(the_message)) => {
                                        warn!("downstream receiver did not pick put message for {}ms - keep waiting", warning_threshold.as_millis());

                                        match sender.send(the_message).await {
                                            Ok(()) => {
                                                messages_forwarded += 1;
                                                trace!("queued delayed update message {} in {:.02}ms",
                                                    messages_forwarded, started_at.elapsed().as_secs_f32() * 1000.0);
                                            }
                                            Err(_send_error  ) => {
                                                warn!("downstream receiver closed, message is lost - aborting");
                                                break 'recv_loop State::FatalError(attempt, FatalErrorReason::DownstreamChannelClosed);
                                            }
                                        }

                                    }
                                    Err(SendTimeoutError::Closed(_)) => {
                                        warn!("downstream receiver closed - aborting");
                                        break 'recv_loop State::FatalError(attempt, FatalErrorReason::DownstreamChannelClosed);
                                    }
                                }
                                // {
                                //     Ok(n_subscribers) => {
                                //         trace!(
                                //             "sent update message to {} subscribers (buffer={})",
                                //             n_subscribers,
                                //             sender.len()
                                //         );
                                //         continue 'recv_loop;
                                //     }
                                //     Err(SendError(_)) => {
                                //         // note: error does not mean that future sends will also fail!
                                //         trace!("no subscribers for update message");
                                //         continue 'recv_loop;
                                //     }
                                // };
                            }
                            Some(Err(tonic_status)) => {
                                // all tonic errors are recoverable
                                warn!("! error on {} - retrying: {:?}", grpc_source, tonic_status);
                                break 'recv_loop State::WaitReconnect(attempt);
                            }
                            None => {
                                warn!("geyser stream closed on {} - retrying", grpc_source);
                                break 'recv_loop State::WaitReconnect(attempt);
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
