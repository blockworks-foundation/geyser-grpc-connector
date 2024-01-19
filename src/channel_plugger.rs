use log::{debug, info, warn};
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::time::{sleep, timeout};


pub fn spawn_plugger_mpcs_to_broadcast(
    mut upstream: tokio::sync::mpsc::Receiver<u64>,
    downstream: tokio::sync::broadcast::Sender<u64>,
) {
    // abort forwarder by closing the sender
    let _donothing = tokio::spawn(async move {
        while let Some(value) = upstream.recv().await {
            match downstream.send(value) {
                Ok(n_subscribers) => {
                    debug!("forwarded to {} subscribers", n_subscribers);
                }
                Err(_dropped_msg) => {
                    // decide to continue if no subscribers
                    debug!("no subscribers - dropping payload and continue");
                }
            }
        }
        debug!("no more messages from producer - shutting down connector");
    });
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_broadcast_to_mpsc() {
        solana_logger::setup_with_default("debug");

        let (tx1, rx1) = tokio::sync::mpsc::channel::<u64>(1);
        let (tx2, rx2) = tokio::sync::broadcast::channel::<u64>(2);
        drop(rx2);

        let jh_producer = tokio::spawn(async move {
            for i in 1..=10 {
                info!("producer sending {}", i);
                if let Err(SendTimeoutError::Timeout(message)) =
                    tx1.send_timeout(i, Duration::from_millis(200)).await
                {
                    info!("producer send was blocked");
                    tx1.send(message).await.unwrap();
                }
                sleep(Duration::from_millis(500)).await;
            }
        });

        // downstream receiver A connected to broadcast
        let mut channel_a = tx2.subscribe();
        tokio::spawn(async move {
            loop {
                match channel_a.recv().await {
                    Ok(msg) => {
                        info!("A: {:?} (len={})", msg, channel_a.len());
                    }
                    Err(RecvError::Lagged(n_missed)) => {
                        warn!("channel A lagged {} messages", n_missed);
                    }
                    Err(RecvError::Closed) => {
                        info!("channel A closed (by forwarder)");
                        break;
                    }
                }
            }
        });

        // downstream receiver B connected to broadcast
        let mut channel_b = tx2.subscribe();
        tokio::spawn(async move {
            loop {
                match channel_b.recv().await {
                    Ok(msg) => {
                        info!("B: {:?} (len={})", msg, channel_b.len());
                        // slow receiver
                        sleep(Duration::from_millis(1000)).await;
                    }
                    Err(RecvError::Lagged(n_missed)) => {
                        warn!("channel B lagged {} messages", n_missed);
                    }
                    Err(RecvError::Closed) => {
                        info!("channel B closed (by forwarder)");
                        break;
                    }
                }
            }
        });

        // connect them
        spawn_plugger_mpcs_to_broadcast(rx1, tx2);

        // wait forever
        info!("Started tasks .. waiting for producer to finish");
        // should take 5 secs
        assert!(
            timeout(Duration::from_secs(10), jh_producer).await.is_ok(),
            "timeout"
        );
        info!("producer done - wait a bit longer ...");
        sleep(Duration::from_secs(3)).await;
        info!("done.");

        // note how messages pile up for slow receiver B
    }

    #[tokio::test]
    async fn connect_broadcast_to_mpsc_nosubscribers() {
        solana_logger::setup_with_default("debug");

        let (tx1, rx1) = tokio::sync::mpsc::channel::<u64>(1);
        let (tx2, rx2) = tokio::sync::broadcast::channel::<u64>(2);

        let jh_producer = tokio::spawn(async move {
            for i in 1..=10 {
                info!("producer sending {}", i);
                if let Err(SendTimeoutError::Timeout(message)) =
                    tx1.send_timeout(i, Duration::from_millis(200)).await
                {
                    info!("producer send was blocked");
                    tx1.send(message).await.unwrap();
                }
                sleep(Duration::from_millis(500)).await;
            }
        });

        // connect them
        spawn_plugger_mpcs_to_broadcast(rx1, tx2);

        sleep(Duration::from_secs(3)).await;
        info!("dropping subscriber");
        drop(rx2);

        // wait forever
        info!("Started tasks .. waiting for producer to finish");
        // should take 5 secs
        assert!(
            timeout(Duration::from_secs(10), jh_producer).await.is_ok(),
            "timeout"
        );
        info!("producer done - wait a bit longer ...");
        sleep(Duration::from_secs(3)).await;
        info!("done.");

        // note how messages pile up for slow receiver B
    }
}
