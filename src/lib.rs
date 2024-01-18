use std::fmt::{Debug, Display};
use std::time::Duration;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;

pub mod grpc_subscription;
pub mod grpc_subscription_autoreconnect_streams;
pub mod grpc_subscription_autoreconnect_tasks;
pub mod grpcmultiplex_fastestwins;
mod obfuscate;


#[derive(Clone, Debug)]
pub struct GrpcConnectionTimeouts {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub subscribe_timeout: Duration,
}

#[derive(Clone)]
pub struct GrpcSourceConfig {
    grpc_addr: String,
    grpc_x_token: Option<String>,
    tls_config: Option<ClientTlsConfig>,
    timeouts: Option<GrpcConnectionTimeouts>,
}

impl Display for GrpcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "grpc_addr {}",
            crate::obfuscate::url_obfuscate_api_token(&self.grpc_addr)
        )
    }
}

impl Debug for GrpcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl GrpcSourceConfig {
    /// Create a grpc source without tls and timeouts
    pub fn new_simple(grpc_addr: String) -> Self {
        Self {
            grpc_addr,
            grpc_x_token: None,
            tls_config: None,
            timeouts: None,
        }
    }
    pub fn new(
        grpc_addr: String,
        grpc_x_token: Option<String>,
        tls_config: Option<ClientTlsConfig>,
        timeouts: GrpcConnectionTimeouts,
    ) -> Self {
        Self {
            grpc_addr,
            grpc_x_token,
            tls_config,
            timeouts: Some(timeouts),
        }
    }
}
