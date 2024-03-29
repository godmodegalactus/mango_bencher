use jsonrpc_core::futures::StreamExt;
use jsonrpc_core_client::transports::{http, ws};

use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_response::{OptionalContext, Response, RpcKeyedAccount},
};
use solana_rpc::{rpc::rpc_accounts_scan::AccountsScanClient, rpc_pubsub::RpcSolPubSubClient};
use solana_sdk::{account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey};

use log::*;
use std::{
    collections::HashSet,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    chain_data::{AccountWrite, SlotStatus, SlotUpdate},
    grpc_plugin_source::FilterConfig,
    AnyhowWrap,
};

enum WebsocketMessage {
    SingleUpdate(Response<RpcKeyedAccount>),
    SnapshotUpdate(Response<Vec<RpcKeyedAccount>>),
    SlotUpdate(Arc<solana_client::rpc_response::SlotUpdate>),
}

#[derive(Debug, Clone)]
pub struct KeeperConfig {
    pub program_id: Pubkey,
    pub rpc_url: String,
    pub websocket_url: String,
}

// TODO: the reconnecting should be part of this
async fn feed_data(
    config: KeeperConfig,
    _filter_config: &FilterConfig,
    sender: async_channel::Sender<WebsocketMessage>,
) -> anyhow::Result<()> {
    let program_id = config.program_id;
    let snapshot_duration = Duration::from_secs(10);

    info!("feed_data {config:?}");
    let connect = ws::try_connect::<RpcSolPubSubClient>(&config.websocket_url).map_err_anyhow()?;
    let client: RpcSolPubSubClient = connect.await.map_err_anyhow()?;

    let rpc_client = http::connect_with_options::<AccountsScanClient>(&config.rpc_url, true)
        .await
        .map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::processed()),
        data_slice: None,
        min_context_slot: None,
    };
    let program_accounts_config = RpcProgramAccountsConfig {
        filters: None,
        with_context: Some(true),
        account_config: account_info_config.clone(),
    };

    let mut update_sub = client
        .program_subscribe(
            program_id.to_string(),
            Some(program_accounts_config.clone()),
        )
        .map_err_anyhow()?;

    let mut slot_sub = client.slots_updates_subscribe().map_err_anyhow()?;

    let mut last_snapshot = Instant::now() - snapshot_duration;

    loop {
        // occasionally cause a new snapshot to be produced
        // including the first time
        if last_snapshot + snapshot_duration <= Instant::now() {
            let account_snapshot = rpc_client
                .get_program_accounts(
                    program_id.to_string(),
                    Some(program_accounts_config.clone()),
                )
                .await
                .map_err_anyhow()?;
            if let OptionalContext::Context(account_snapshot_response) = account_snapshot {
                info!("snapshot");
                sender
                    .send(WebsocketMessage::SnapshotUpdate(account_snapshot_response))
                    .await
                    .expect("sending must succeed");
            }
            last_snapshot = Instant::now();
        }

        tokio::select! {
            account = update_sub.next() => {
              trace!("account {account:?}");
                match account {
                    Some(account) => {
                        sender.send(WebsocketMessage::SingleUpdate(account.map_err_anyhow()?)).await.expect("sending must succeed");
                    },
                    None => {
                        warn!("account stream closed");
                        return Ok(());
                    },
                }
            },
            slot_update = slot_sub.next() => {
                trace!("slot {slot_update:?}");
                match slot_update {
                    Some(slot_update) => {
                        sender.send(WebsocketMessage::SlotUpdate(slot_update.map_err_anyhow()?)).await.expect("sending must succeed");
                    },
                    None => {
                        warn!("slot update stream closed");
                        return Ok(());
                    },
                }
            },
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                warn!("websocket timeout");
                return Ok(())
            }
        }
    }
}

// TODO: rename / split / rework
pub async fn process_events(
    config: KeeperConfig,
    filter_config: &FilterConfig,
    account_write_queue_sender: async_channel::Sender<AccountWrite>,
    slot_queue_sender: async_channel::Sender<SlotUpdate>,
) {
    let account_wl = HashSet::<String>::from_iter(filter_config.account_ids.iter().cloned());

    // Subscribe to program account updates websocket
    let (update_sender, update_receiver) = async_channel::unbounded::<WebsocketMessage>();
    let filter_config = filter_config.clone();
    tokio::spawn(async move {
        // if the websocket disconnects, we get no data in a while etc, reconnect and try again
        loop {
            let out = feed_data(config.clone(), &filter_config, update_sender.clone());
            let res = out.await;
            info!("loop {res:?}");
        }
    });

    // The thread that pulls updates and forwards them to the account write queue
    loop {
        let update = update_receiver.recv().await.unwrap();

        match update {
            WebsocketMessage::SingleUpdate(update) => {
                if !account_wl.is_empty() && !account_wl.contains(&update.value.pubkey) {
                    continue;
                }
                trace!("single update");
                let account: Account = update.value.account.decode().unwrap();
                let pubkey = Pubkey::from_str(&update.value.pubkey).unwrap();
                account_write_queue_sender
                    .send(AccountWrite::from(pubkey, update.context.slot, 0, account))
                    .await
                    .expect("send success");
            }
            WebsocketMessage::SnapshotUpdate(update) => {
                trace!("snapshot update");
                for keyed_account in update.value {
                    if !account_wl.is_empty() && !account_wl.contains(&keyed_account.pubkey) {
                        continue;
                    }
                    let account: Account = keyed_account.account.decode().unwrap();
                    let pubkey = Pubkey::from_str(&keyed_account.pubkey).unwrap();
                    account_write_queue_sender
                        .send(AccountWrite::from(pubkey, update.context.slot, 0, account))
                        .await
                        .expect("send success");
                }
            }
            WebsocketMessage::SlotUpdate(update) => {
                trace!("slot update");
                let message = match *update {
                    solana_client::rpc_response::SlotUpdate::CreatedBank {
                        slot, parent, ..
                    } => Some(SlotUpdate {
                        slot,
                        parent: Some(parent),
                        status: SlotStatus::Processed,
                    }),
                    solana_client::rpc_response::SlotUpdate::OptimisticConfirmation {
                        slot,
                        ..
                    } => Some(SlotUpdate {
                        slot,
                        parent: None,
                        status: SlotStatus::Confirmed,
                    }),
                    solana_client::rpc_response::SlotUpdate::Root { slot, .. } => {
                        Some(SlotUpdate {
                            slot,
                            parent: None,
                            status: SlotStatus::Rooted,
                        })
                    }
                    _ => None,
                };
                if let Some(message) = message {
                    slot_queue_sender.send(message).await.expect("send success");
                }
            }
        }
    }
}
