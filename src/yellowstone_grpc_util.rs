use std::time::Duration;
use yellowstone_grpc_client::{GeyserGrpcBuilder, GeyserGrpcBuilderError, GeyserGrpcBuilderResult, GeyserGrpcClient, GeyserGrpcClientError, GeyserGrpcClientResult, InterceptorXToken};
use yellowstone_grpc_proto::geyser::SubscribeRequest;
use yellowstone_grpc_proto::prost::bytes::Bytes;
use tonic;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::transport::ClientTlsConfig;

pub type GeyserGrpcWrappedResult<T> = Result<T, GrpcErrorWrapper>;

#[derive(Debug)]
pub enum GrpcErrorWrapper {
    GrpcClientError(GeyserGrpcClientError),
    GeyserGrpcBuilderError(GeyserGrpcBuilderError),
}

const MAX_DECODING_MESSAGE_SIZE_BYTES: usize = 16 * 1024 * 1024;
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
                buffer_size: Some(65536),     // 64kb (default: 1k)
                conn_window: Some(5242880),   // 5mb (=default)
                stream_window: Some(4194304), // 4mb (default: 2m)
            }
        } else {
            GeyserGrpcClientBufferConfig::default()
        }
    }
}


/*
async fn create_connection(
    grpc_config: &GrpcSourceConfig,
) -> anyhow::Result<GeyserGrpcClient<impl Interceptor + Sized>> {
    connect_with_timeout_with_buffers(
        // ...
    ).await
    .map_err(|e| {
        anyhow!("Failed to connect to grpc source: {e:?}")
    })
}
*/

/// This function is used to create a connection to a gRPC server.
///
/// Suggest to use `anyhow` to wrap the opaque error type - see create_connection.
pub async fn connect_with_timeout_with_buffers<E, T>(
    endpoint: E,
    x_token: Option<T>,
    tls_config: Option<ClientTlsConfig>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    buffer_config: GeyserGrpcClientBufferConfig,
) -> GeyserGrpcBuilderResult<GeyserGrpcClient<impl Interceptor>>
where
    E: Into<Bytes>,
    T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
{
    let mut builder = GeyserGrpcBuilder::from_shared(endpoint)?
        .x_token(x_token)?
        .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE_BYTES)
        .buffer_size(buffer_config.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE))
        .initial_connection_window_size(buffer_config.conn_window.unwrap_or(DEFAULT_CONN_WINDOW))
        .initial_stream_window_size(buffer_config.stream_window.unwrap_or(DEFAULT_STREAM_WINDOW));

    if let Some(tls_config) = tls_config {
        builder = builder.tls_config(tls_config)?;
    }

    if let Some(connect_timeout) = connect_timeout {
        builder = builder.timeout(connect_timeout);
    }

    if let Some(request_timeout) = request_timeout {
        builder = builder.timeout(request_timeout);
    }

    let client = builder.connect_lazy()?;
    Ok(client)
}
