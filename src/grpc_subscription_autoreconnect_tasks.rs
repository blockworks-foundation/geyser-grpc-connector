use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::channel::mpsc::SendError;
use futures::{Sink, SinkExt, Stream, StreamExt};
use log::{debug, error, info, log, trace, warn, Level};
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};
use yellowstone_grpc_client::{GeyserGrpcBuilderError, GeyserGrpcClient, GeyserGrpcClientError};
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeUpdate,
};
use yellowstone_grpc_proto::tonic::service::Interceptor;
use yellowstone_grpc_proto::tonic::Status;

use crate::yellowstone_grpc_util::{
    connect_with_timeout_with_buffers, GeyserGrpcClientBufferConfig,
};
use crate::{Attempt, GrpcSourceConfig, Message};

enum ConnectionState<
    S: Stream<Item = Result<SubscribeUpdate, Status>>,
    F: Interceptor,
    R: Sink<SubscribeRequest, Error = futures::channel::mpsc::SendError>,
> {
    NotConnected(Attempt),
    // connected but not subscribed
    Connecting(Attempt, GeyserGrpcClient<F>),
    Ready(S, R),
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
) -> (JoinHandle<()>, Receiver<Message>, Sender<SubscribeRequest>) {
    let (sender, receiver_channel) = tokio::sync::mpsc::channel::<Message>(1);

    // TODO rename
    let (join_handle, client_subscribe_tx) = create_geyser_autoconnection_task_with_mpsc(
        grpc_source,
        subscribe_filter,
        sender,
        exit_notify,
    );

    (join_handle, receiver_channel, client_subscribe_tx)
}

/// connect to grpc source performing autoconnect if required,
/// returns mpsc channel; task will abort on fatal error
/// will shut down when receiver is dropped
pub fn create_geyser_autoconnection_task_with_mpsc(
    grpc_source: GrpcSourceConfig,
    subscribe_filter: SubscribeRequest,
    mpsc_downstream: tokio::sync::mpsc::Sender<Message>,
    mut exit_notify: broadcast::Receiver<()>,
) -> (JoinHandle<()>, Sender<SubscribeRequest>) {
    // read this for argument: http://www.randomhacks.net/2019/03/08/should-rust-channels-panic-on-send/
    let (client_subscribe_tx, mut client_subscribe_rx) =
        tokio::sync::mpsc::channel::<SubscribeRequest>(1);

    // task will be aborted when downstream receiver gets dropped
    // there are two ways to terminate: 1) using break 'main_loop 2) return from task
    let jh_geyser_task = tokio::spawn(async move {
        // use this filter for initial connect and update it if the client requests a change via client_subscribe_tx channel
        let mut subscribe_filter_on_connect = subscribe_filter;
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
                        Err(GeyserGrpcBuilderError::MetadataValueError(_)) => {
                            ConnectionState::FatalError(
                                attempt + 1,
                                FatalErrorReason::ConfigurationError,
                            )
                        }
                        Err(GeyserGrpcBuilderError::InvalidXTokenLength(_)) => {
                            ConnectionState::FatalError(
                                attempt + 1,
                                FatalErrorReason::ConfigurationError,
                            )
                        }
                        Err(GeyserGrpcBuilderError::TonicError(tonic_error)) => {
                            warn!(
                                "connect failed on {} - aborting: {:?}",
                                grpc_source, tonic_error
                            );
                            ConnectionState::FatalError(attempt + 1, FatalErrorReason::NetworkError)
                        }
                        Err(GeyserGrpcBuilderError::EmptyChannel) => {
                            warn!(
                                "connect failed on {} - tonic::transport::Channel should be created, use `connect` or `connect_lazy` first",
                                grpc_source,
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
                        MaybeExit::Continue(connection_result) => {
                            connection_handler(connection_result)
                        }
                        MaybeExit::Exit => ConnectionState::GracefulShutdown,
                    }
                }
                ConnectionState::Connecting(attempt, mut client) => {
                    let subscribe_timeout =
                        grpc_source.timeouts.as_ref().map(|t| t.subscribe_timeout);
                    let subscribe_filter_on_connect = subscribe_filter_on_connect.clone();
                    debug!("Subscribe initially with filter {:?}", subscribe_filter_on_connect);

                    let fut_subscribe = timeout(
                        subscribe_timeout.unwrap_or(Duration::MAX),
                        client.subscribe_with_request(Some(subscribe_filter_on_connect)),
                    );

                    match await_or_exit(fut_subscribe, exit_notify.recv()).await {
                        MaybeExit::Continue(subscribe_result_timeout) => {
                            match subscribe_result_timeout {
                                Ok(subscribe_result) => {
                                    match subscribe_result {
                                        Ok((geyser_subscribe_tx, geyser_stream)) => {
                                            if attempt > 1 {
                                                debug!(
                                                    "Subscribed to {} after {} failed attempts",
                                                    grpc_source, attempt
                                                );
                                            }
                                            ConnectionState::Ready(
                                                geyser_stream,
                                                geyser_subscribe_tx,
                                            )
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
                        MaybeExit::Exit => ConnectionState::GracefulShutdown,
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
                        MaybeExit::Exit => ConnectionState::GracefulShutdown,
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
                        MaybeExit::Exit => ConnectionState::GracefulShutdown,
                    }
                }
                ConnectionState::Ready(mut geyser_stream, mut geyser_subscribe_tx) => {
                    let receive_timeout = grpc_source.timeouts.as_ref().map(|t| t.receive_timeout);
                    'recv_loop: loop {
                        select! {
                             exit_res = exit_notify.recv() => {
                                match exit_res {
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
                                break 'recv_loop ConnectionState::GracefulShutdown;
                            },
                            client_subscribe_update = client_subscribe_rx.recv() => {
                                match client_subscribe_update {
                                    Some(subscribe_request) => {
                                        debug!("Subscription update from client with filter {:?}", subscribe_request);
                                        subscribe_filter_on_connect = subscribe_request.clone();
                                        // note: if the subscription is invalid, it will trigger a Tonic error:
                                        //  Status { code: InvalidArgument, message: "failed to create filter: Invalid Base58 string", source: None }
                                        if let Err(send_err) = geyser_subscribe_tx.send(subscribe_request).await {
                                            warn!("fail to send subscription update - disconnect and retry");
                                            break 'recv_loop ConnectionState::WaitReconnect(1);
                                        };
                                    }
                                    None => {
                                        trace!("client subscribe channel closed, continue without");
                                        continue 'recv_loop;
                                    }
                                }
                            },
                            geyser_stream_res = timeout(
                                    receive_timeout.unwrap_or(Duration::MAX),
                                    geyser_stream.next(),
                                ) => {

                                // let MaybeExit::Continue(geyser_stream_res) =
                                //     await_or_exit(fut_stream, exit_notify.recv()).await
                                // else {
                                    // break 'recv_loop ConnectionState::GracefulShutdown;
                                // };

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

                                        let MaybeExit::Continue(mpsc_downstream_result) =
                                            await_or_exit(fut_send, exit_notify.recv()).await
                                        else {
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

                                                let MaybeExit::Continue(mpsc_downstream_result) =
                                                    await_or_exit(fut_send, exit_notify.recv()).await
                                                else {
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
                                        warn!("tonic error on {} - retrying: {:?}", grpc_source, tonic_status);
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

                            },
                        }
                    } // -- END receive loop
                }
                ConnectionState::GracefulShutdown => {
                    debug!("shutting down {} gracefully on exit signal", grpc_source);
                    break 'main_loop;
                }
            } // -- END match
        } // -- state loop; break ONLY on graceful shutdown
        debug!("gracefully exiting geyser task loop");
    });

    (jh_geyser_task, client_subscribe_tx)
}

fn buffer_config_from_env() -> GeyserGrpcClientBufferConfig {
    if env::var("BUFFER_SIZE").is_err()
        || env::var("CONN_WINDOW").is_err()
        || env::var("STREAM_WINDOW").is_err()
    {
        warn!("BUFFER_SIZE, CONN_WINDOW, STREAM_WINDOW not set; using default buffer config");
        return GeyserGrpcClientBufferConfig::default();
    }

    let buffer_size = env::var("BUFFER_SIZE")
        .expect("buffer_size")
        .parse::<usize>()
        .expect("integer(bytes)");
    let conn_window = env::var("CONN_WINDOW")
        .expect("conn_window")
        .parse::<u32>()
        .expect("integer(bytes)");
    let stream_window = env::var("STREAM_WINDOW")
        .expect("stream_window")
        .parse::<u32>()
        .expect("integer(bytes)");

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
