use std::collections::HashMap;
use std::env;

use log::info;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::signature::Signature;
use tokio::sync::broadcast;
use tokio::time::Duration;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterTransactions};

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{
    map_commitment_level, GrpcConnectionTimeouts, GrpcSourceConfig, Message,
};

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();

    let (_foo, exit_notify) = broadcast::channel(1);

    info!(
        "Using gRPC source {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    let green_config =
        GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, None, timeouts.clone());

    let (autoconnect_tx, mut transactions_rx) = tokio::sync::mpsc::channel(10);
    let _tx_source_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        jupyter_trades(),
        autoconnect_tx.clone(),
        exit_notify,
    );

    loop {
        let message = transactions_rx.recv().await;
        if let Some(Message::GeyserSubscribeUpdate(update)) = message {
            match update.update_oneof {
                Some(UpdateOneof::Transaction(update)) => {
                    let tx = update.transaction.unwrap();
                    let sig = Signature::try_from(tx.signature.as_slice()).unwrap();
                    info!("tx {}", sig);
                }
                _ => unimplemented!(),
            }
        }
    }
}

fn jupyter_trades() -> SubscribeRequest {
    let mut transaction_subs = HashMap::new();
    transaction_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec!["JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string()],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeRequest {
        transactions: transaction_subs,
        ping: None,
        commitment: Some(map_commitment_level(CommitmentConfig {
            commitment: CommitmentLevel::Confirmed,
        }) as i32),
        ..Default::default()
    }
}
