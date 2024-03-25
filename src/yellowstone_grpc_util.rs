use std::time::Duration;
use log::info;
use tonic_health::pb::health_client::HealthClient;
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientResult, InterceptorXToken};
use yellowstone_grpc_proto::geyser::geyser_client::GeyserClient;
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

#[derive(Debug)]
pub struct EndpointConfig {
    // 65536
    pub buffer_size: usize,
    // 4194304
    pub initial_connection_window_size: u32,
    // 4194304
    pub initial_stream_window_size: u32,
}

pub async fn connect_with_timeout_hacked<E, T>(endpoint_config: EndpointConfig, endpoint: E,
                    x_token: Option<T>,)  -> GeyserGrpcClientResult<GeyserGrpcClient<impl Interceptor>>
    where
        E: Into<Bytes>,
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>, {

    info!("endpoint config: {:?}", endpoint_config);

    let endpoint = tonic::transport::Endpoint::from_shared(endpoint).unwrap() // FIXME
        .http2_adaptive_window(false)
        .buffer_size(Some(endpoint_config.buffer_size))
        .initial_connection_window_size(Some(endpoint_config.initial_connection_window_size))
        .initial_stream_window_size(Some(endpoint_config.initial_stream_window_size))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))

        .tls_config(ClientTlsConfig::new()).unwrap(); // FIXME

    let x_token: Option<AsciiMetadataValue> = match x_token {
        Some(x_token) => Some(x_token.try_into().unwrap()), // FIXME replace unwrap
        None => None,
    };
    // match x_token {
    //     Some(token) if token.is_empty() => {
    //         panic!("empty token");
    //     }
    //     _ => {}
    // }
    let interceptor = InterceptorXToken { x_token };

    let channel = endpoint.connect_lazy();
    let mut client = GeyserGrpcClient::new(
        // TODO move tonic-health
        HealthClient::with_interceptor(channel.clone(), interceptor.clone()),
        GeyserClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(GeyserGrpcClient::max_decoding_message_size()),
    );
    Ok(client)
}