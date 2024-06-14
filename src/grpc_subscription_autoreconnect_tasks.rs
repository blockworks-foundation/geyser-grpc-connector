use std::env;
use std::future::Future;
use crate::{yellowstone_grpc_util, Attempt, GrpcSourceConfig, Message};
use futures::{Stream, StreamExt};
use log::{debug, error, info, log, trace, warn, Level};
use std::time::Duration;
use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
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
    // exit signal received
    GracefulShutdown,
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
    exit_notify: broadcast::Receiver<()>,
) -> (JoinHandle<()>, Receiver<Message>) {
    let (sender, receiver_channel) = tokio::sync::mpsc::channel::<Message>(1);

    let join_handle = create_geyser_autoconnection_task_with_mpsc(
        grpc_source,
        subscribe_filter,
        sender,
        exit_notify,
    );

    (join_handle, receiver_channel)
}

/// connect to grpc source performing autoconnect if required,
/// returns mpsc channel; task will abort on fatal error
/// will shut down when receiver is dropped
pub fn create_geyser_autoconnection_task_with_mpsc(
    grpc_source: GrpcSourceConfig,
    subscribe_filter: SubscribeRequest,
    mpsc_downstream: tokio::sync::mpsc::Sender<Message>,
    mut exit_notify: broadcast::Receiver<()>,
) -> JoinHandle<()> {
    // read this for argument: http://www.randomhacks.net/2019/03/08/should-rust-channels-panic-on-send/

    // task will be aborted when downstream receiver gets dropped
    let jh_geyser_task = tokio::spawn(async move {
        let mut state = ConnectionState::NotConnected(1);
        let mut messages_forwarded = 0;

        'main_loop: loop {
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

                    // let buffer_config = yellowstone_grpc_util::GeyserGrpcClientBufferConfig::optimize_for_subscription(&subscribe_filter);
                    let buffer_config = buffer_config_from_env();
                    debug!("Using Grpc Buffer config {:?}", buffer_config);

                    let connection_handler = |connect_result| match connect_result {
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
                    };

                    let fut_connector = connect_with_timeout_with_buffers(
                        addr,
                        token,
                        config,
                        connect_timeout,
                        request_timeout,
                        buffer_config,
                    );

                    match await_or_exit(fut_connector, exit_notify.recv()).await {
                        MaybeExit::Continue(connection_result) => connection_handler(connection_result),
                        MaybeExit::Exit => ConnectionState::GracefulShutdown
                    }

                }
                ConnectionState::Connecting(attempt, mut client) => {
                    let subscribe_timeout =
                        grpc_source.timeouts.as_ref().map(|t| t.subscribe_timeout);
                    let subscribe_filter = subscribe_filter.clone();
                    debug!("Subscribe with filter {:?}", subscribe_filter);

                    let subscribe_handler = |subscribe_result_timeout| match subscribe_result_timeout {
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
                    };

                    let fut_subscribe = timeout(
                        subscribe_timeout.unwrap_or(Duration::MAX),
                        client.subscribe_once2(subscribe_filter),
                    );

                    match await_or_exit(fut_subscribe, exit_notify.recv()).await {
                        MaybeExit::Continue(subscribe_result_timeout) => subscribe_handler(subscribe_result_timeout),
                        MaybeExit::Exit => ConnectionState::GracefulShutdown
                    }

                }
                ConnectionState::RecoverableConnectionError(attempt) => {
                    let backoff_secs = 1.5_f32.powi(attempt as i32).min(15.0);
                    info!(
                        "waiting {} seconds, then reconnect to {}",
                        backoff_secs, grpc_source
                    );

                    let fut_sleep = sleep(Duration::from_secs_f32(backoff_secs));

                    match await_or_exit(fut_sleep, exit_notify.recv()).await {
                        MaybeExit::Continue(()) => ConnectionState::NotConnected(attempt),
                        MaybeExit::Exit => ConnectionState::GracefulShutdown
                    }
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

                    let fut_sleep = sleep(Duration::from_secs_f32(backoff_secs));

                    match await_or_exit(fut_sleep, exit_notify.recv()).await {
                        MaybeExit::Continue(()) => ConnectionState::NotConnected(attempt),
                        MaybeExit::Exit => ConnectionState::GracefulShutdown
                    }
                }
                ConnectionState::Ready(mut geyser_stream) => {
                    let receive_timeout = grpc_source.timeouts.as_ref().map(|t| t.receive_timeout);
                    'recv_loop: loop {

                        let fut_stream = timeout(
                            receive_timeout.unwrap_or(Duration::MAX),
                            geyser_stream.next(),
                        );

                        let MaybeExit::Continue(geyser_stream_res) = await_or_exit(fut_stream, exit_notify.recv()).await else {
                            break 'recv_loop ConnectionState::GracefulShutdown;
                        };

                        match geyser_stream_res {
                            Ok(Some(Ok(update_message))) => {
                                trace!("> recv update message from {}", grpc_source);
                                // note: first send never blocks as the mpsc channel has capacity 1
                                let warning_threshold = if messages_forwarded == 1 {
                                    Duration::from_millis(3000)
                                } else {
                                    Duration::from_millis(500)
                                };
                                let started_at = Instant::now();

                                let fut_send = mpsc_downstream.send_timeout(
                                    Message::GeyserSubscribeUpdate(Box::new(update_message)),
                                    warning_threshold,
                                );

                                let MaybeExit::Continue(mpsc_downstream_result) = await_or_exit(
                                    fut_send,
                                    exit_notify.recv()).await else {
                                    break 'recv_loop ConnectionState::GracefulShutdown;
                                };

                                match mpsc_downstream_result {
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

                                        let fut_send = mpsc_downstream.send(the_message);

                                        let MaybeExit::Continue(mpsc_downstream_result) = await_or_exit(
                                            fut_send,
                                            exit_notify.recv()).await else {
                                            break 'recv_loop ConnectionState::GracefulShutdown;
                                        };

                                        match mpsc_downstream_result {
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
                        }; // -- END match

                    } // -- END receive loop
                }
                ConnectionState::GracefulShutdown => {
                    debug!("shutting down {} gracefully on exit signal", grpc_source);
                    break 'main_loop;
                }
            } // -- END match
        } // -- state loop; break ONLY on graceful shutdown
    });

    jh_geyser_task
}

fn buffer_config_from_env() -> GeyserGrpcClientBufferConfig {
    if env::var("BUFFER_SIZE").is_err() || env::var("CONN_WINDOW").is_err() || env::var("STREAM_WINDOW").is_err() {
        return GeyserGrpcClientBufferConfig::default();
    }

    let buffer_size = env::var("BUFFER_SIZE").expect("buffer_size").parse::<usize>().expect("integer(bytes)");
    let conn_window = env::var("CONN_WINDOW").expect("conn_window").parse::<u32>().expect("integer(bytes)");
    let stream_window = env::var("STREAM_WINDOW").expect("stream_window").parse::<u32>().expect("integer(bytes)");

    // conn_window should be larger than stream_window
    GeyserGrpcClientBufferConfig {
        buffer_size: Some(buffer_size),
        conn_window: Some(conn_window),
        stream_window: Some(stream_window),
    }
}


enum MaybeExit<T> {
    Continue(T),
    Exit,
}

async fn await_or_exit<F, E, T>(future: F, exit_notify: E) -> MaybeExit<F::Output>
    where
        F: Future,
        E: Future<Output = Result<T, RecvError>>,
{
    tokio::select! {
        res = future => {
            MaybeExit::Continue(res)
        },
        res = exit_notify => {
            match res {
                Ok(_) => {
                    debug!("exit on signal");
                }
                 Err(RecvError::Lagged(_)) => {
                    warn!("exit on signal (lag)");
                }
                Err(RecvError::Closed) => {
                    warn!("exit on signal (channel close)");
                }
            }
            MaybeExit::Exit
        }
    }
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
