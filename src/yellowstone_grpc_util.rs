use std::time::Duration;
use tonic_health::pb::health_client::HealthClient;
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientResult, InterceptorXToken};
use yellowstone_grpc_proto::geyser::geyser_client::GeyserClient;
use yellowstone_grpc_proto::geyser::SubscribeRequest;
use yellowstone_grpc_proto::prost::bytes::Bytes;
use yellowstone_grpc_proto::tonic;
use yellowstone_grpc_proto::tonic::metadata::AsciiMetadataValue;
use yellowstone_grpc_proto::tonic::metadata::errors::InvalidMetadataValue;
use yellowstone_grpc_proto::tonic::service::Interceptor;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;


pub async fn connect_with_timeout<E, T>(
    endpoint: E,
    x_token: Option<T>,
    tls_config: Option<ClientTlsConfig>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    connect_lazy: bool,
) -> GeyserGrpcClientResult<GeyserGrpcClient<impl Interceptor>>
    where
        E: Into<Bytes>,
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
{
    GeyserGrpcClient::connect_with_timeout(
       endpoint, x_token, tls_config, connect_timeout, request_timeout, connect_lazy).await
}


// see https://github.com/hyperium/tonic/blob/v0.10.2/tonic/src/transport/channel/mod.rs
const DEFAULT_BUFFER_SIZE: usize = 1024;
// see https://github.com/hyperium/hyper/blob/v0.14.28/src/proto/h2/client.rs#L45
const DEFAULT_CONN_WINDOW: u32 = 1024 * 1024 * 5; // 5mb
const DEFAULT_STREAM_WINDOW: u32 = 1024 * 1024 * 2; // 2mb

#[derive(Debug, Clone)]
pub struct GeyserGrpcClientBufferConfig {
    pub buffer_size: Option<usize>,
    pub conn_window: Option<u32>,
    pub stream_window: Option<u32>,
}

impl Default for GeyserGrpcClientBufferConfig {
    fn default() -> Self {
        GeyserGrpcClientBufferConfig {
            buffer_size: Some(DEFAULT_BUFFER_SIZE),
            conn_window: Some(DEFAULT_CONN_WINDOW),
            stream_window: Some(DEFAULT_STREAM_WINDOW),
        }
    }
}

impl GeyserGrpcClientBufferConfig {

    pub fn optimize_for_subscription(filter: &SubscribeRequest) -> GeyserGrpcClientBufferConfig {
        if !filter.blocks.is_empty() {
            GeyserGrpcClientBufferConfig {
                buffer_size: Some(65536),  // 64kb (default: 1k)
                conn_window: Some(5242880), // 5mb (=default)
                stream_window: Some(4194304), // 4mb (default: 2m)
            }
        } else {
            GeyserGrpcClientBufferConfig::default()
        }
    }

}


pub async fn connect_with_timeout_with_buffers<E, T>(
    endpoint: E,
    x_token: Option<T>,
    tls_config: Option<ClientTlsConfig>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    buffer_config: GeyserGrpcClientBufferConfig,
) -> GeyserGrpcClientResult<GeyserGrpcClient<impl Interceptor>>
    where
        E: Into<Bytes>,
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>, {
    // see https://github.com/blockworks-foundation/geyser-grpc-connector/issues/10
    let mut endpoint = tonic::transport::Endpoint::from_shared(endpoint)?
        .buffer_size(buffer_config.buffer_size)
        .initial_connection_window_size(buffer_config.conn_window)
        .initial_stream_window_size(buffer_config.stream_window);

    if let Some(tls_config) = tls_config {
        endpoint = endpoint.tls_config(tls_config)?;
    }

    if let Some(connect_timeout) = connect_timeout {
        endpoint = endpoint.timeout(connect_timeout);
    }

    if let Some(request_timeout) = request_timeout {
        endpoint = endpoint.timeout(request_timeout);
    }

    let x_token: Option<AsciiMetadataValue> = match x_token {
        Some(x_token) => Some(x_token.try_into()?),
        None => None,
    };
    let interceptor = InterceptorXToken { x_token };

    let channel = endpoint.connect_lazy();
    let mut client = GeyserGrpcClient::new(
        HealthClient::with_interceptor(channel.clone(), interceptor.clone()),
        GeyserClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(GeyserGrpcClient::max_decoding_message_size()),
    );
    Ok(client)
}