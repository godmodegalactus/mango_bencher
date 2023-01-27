use chrono::{DateTime, Utc};
use fixed::types::I80F48;
use log::*;
use mango::{
    instruction::{cancel_all_perp_orders, place_perp_order2},
    matching::Side,
    state::{MangoCache, MangoGroup, PerpMarket},
};
use mango_common::Loadable;
use serde::Serialize;
use serde_json;
mod rotating_queue;
use rotating_queue::RotatingQueue;

use solana_bench_mango::{
    cli,
    mango::{AccountKeys, MangoConfig},
};
use solana_client::{
    bidirectional_channel_handler::{BidirectionalChannelHandler, QuicHandlerMessage},
    connection_cache::ConnectionCache,
    rpc_client::RpcClient,
    rpc_config::RpcBlockConfig,
    tpu_client::TpuClient,
};
use solana_program::native_token::LAMPORTS_PER_SOL;
use solana_runtime::bank::RewardType;
use solana_sdk::{
    clock::{Slot, DEFAULT_MS_PER_SLOT},
    commitment_config::{CommitmentConfig, CommitmentLevel},
    hash::Hash,
    instruction::Instruction,
    message::Message,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};

use solana_program::pubkey::Pubkey;
use solana_streamer::bidirectional_channel::QuicReplyMessage;
use solana_transaction_status::UiTransactionStatusMeta;

use std::{
    collections::HashMap,
    fs,
    ops::{Div, Mul},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{channel, Receiver, Sender, TryRecvError},
        Arc, RwLock,
    },
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

use csv;

// as there are similar modules solana_sdk and solana_program
// solana internals use solana_sdk but external dependancies like mango use solana program
// that is why we have some helper methods
fn to_sdk_pk(pubkey: &Pubkey) -> solana_sdk::pubkey::Pubkey {
    solana_sdk::pubkey::Pubkey::from(pubkey.to_bytes())
}

fn to_sp_pk(pubkey: &solana_sdk::pubkey::Pubkey) -> Pubkey {
    Pubkey::new_from_array(pubkey.to_bytes())
}

fn to_sdk_accountmetas(
    vec: Vec<solana_program::instruction::AccountMeta>,
) -> Vec<solana_sdk::instruction::AccountMeta> {
    vec.iter()
        .map(|x| solana_sdk::instruction::AccountMeta {
            pubkey: to_sdk_pk(&x.pubkey),
            is_signer: x.is_signer,
            is_writable: x.is_writable,
        })
        .collect::<Vec<solana_sdk::instruction::AccountMeta>>()
}

fn to_sdk_instruction(
    instruction: solana_program::instruction::Instruction,
) -> solana_sdk::instruction::Instruction {
    solana_sdk::instruction::Instruction {
        program_id: to_sdk_pk(&instruction.program_id),
        accounts: to_sdk_accountmetas(instruction.accounts),
        data: instruction.data,
    }
}

fn load_from_rpc<T: Loadable>(rpc_client: &RpcClient, pk: &Pubkey) -> T {
    let acc = rpc_client.get_account(&to_sdk_pk(pk)).unwrap();
    return T::load_from_bytes(acc.data.as_slice()).unwrap().clone();
}

fn get_latest_blockhash(rpc_client: &RpcClient) -> Hash {
    loop {
        match rpc_client.get_latest_blockhash() {
            Ok(blockhash) => return blockhash,
            Err(err) => {
                info!("Couldn't get last blockhash: {:?}", err);
                sleep(Duration::from_secs(1));
            }
        };
    }
}

fn get_new_latest_blockhash(client: &Arc<RpcClient>, blockhash: &Hash) -> Option<Hash> {
    let start = Instant::now();
    while start.elapsed().as_secs() < 5 {
        if let Ok(new_blockhash) = client.get_latest_blockhash() {
            if new_blockhash != *blockhash {
                debug!("Got new blockhash ({:?})", blockhash);
                return Some(new_blockhash);
            }
        }
        debug!("Got same blockhash ({:?}), will retry...", blockhash);

        // Retry ~twice during a slot
        sleep(Duration::from_millis(DEFAULT_MS_PER_SLOT / 2));
    }
    None
}

#[derive(Clone, Serialize)]
struct TransactionSendRecord {
    pub signature: Signature,
    pub sent_at: DateTime<Utc>,
    pub sent_slot: Slot,
    pub market_maker: Pubkey,
    pub market: Pubkey,
}

#[derive(Clone, Serialize)]
struct TransactionRecord {
    pub signature: String,
    pub sent_slot: Slot,
    pub sent_at: String,
    pub confirmed_slot: Option<Slot>,
    pub confirmed_at: Option<String>,
    pub successful: bool,
    pub slot_leader: Option<String>,
    pub error: String,
    pub market_maker: String,
    pub market: String,
    pub block_hash: Option<String>,
    pub slot_processed: Option<Slot>,
    pub timeout: bool,
    pub message_in_bidirectional_replies: Option<String>,
}

impl TransactionRecord {
    pub fn new_confirmed(
        transaction_record: &TransactionSendRecord,
        slot: Slot,
        meta: &Option<UiTransactionStatusMeta>,
        block_hash: String,
        slot_leader: String,
        message_in_bidirectional_replies: Option<String>,
    ) -> Self {
        Self {
            signature: transaction_record.signature.to_string(),
            confirmed_slot: Some(slot), // TODO: should be changed to correct slot
            confirmed_at: Some(Utc::now().to_string()),
            sent_at: transaction_record.sent_at.to_string(),
            sent_slot: transaction_record.sent_slot,
            successful: if let Some(meta) = &meta {
                meta.status.is_ok()
            } else {
                false
            },
            error: if let Some(meta) = &meta {
                match &meta.err {
                    Some(x) => x.to_string(),
                    None => "".to_string(),
                }
            } else {
                "".to_string()
            },
            block_hash: Some(block_hash),
            market: transaction_record.market.to_string(),
            market_maker: transaction_record.market_maker.to_string(),
            slot_processed: Some(slot),
            slot_leader: Some(slot_leader),
            timeout: false,
            message_in_bidirectional_replies,
        }
    }

    pub fn new_errored(
        transaction_record: &TransactionSendRecord,
        error: String,
        message_in_bidirectional_replies: Option<String>,
    ) -> Self {
        Self {
            signature: transaction_record.signature.to_string(),
            confirmed_slot: None, // TODO: should be changed to correct slot
            confirmed_at: None,
            sent_at: transaction_record.sent_at.to_string(),
            sent_slot: transaction_record.sent_slot,
            successful: false,
            error: error,
            block_hash: None,
            market: transaction_record.market.to_string(),
            market_maker: transaction_record.market_maker.to_string(),
            slot_processed: None,
            slot_leader: None,
            timeout: true,
            message_in_bidirectional_replies,
        }
    }
}

#[derive(Clone)]
struct PerpMarketCache {
    pub order_base_lots: i64,
    pub price: I80F48,
    pub price_quote_lots: i64,
    pub mango_program_pk: Pubkey,
    pub mango_group_pk: Pubkey,
    pub mango_cache_pk: Pubkey,
    pub perp_market_pk: Pubkey,
    pub perp_market: PerpMarket,
}

struct _TransactionInfo {
    pub signature: Signature,
    pub transaction_send_time: DateTime<Utc>,
    pub send_slot: Slot,
    pub confirmation_retries: u32,
    pub error: String,
    pub confirmation_blockhash: Pubkey,
    pub leader_confirming_transaction: Pubkey,
    pub timeout: bool,
    pub market_maker: Pubkey,
    pub market: Pubkey,
}

fn poll_blockhash_and_slot(
    exit_signal: &Arc<AtomicBool>,
    blockhash: Arc<RwLock<Hash>>,
    slot: &AtomicU64,
    client: &Arc<RpcClient>,
) {
    let mut blockhash_last_updated = Instant::now();
    //let mut last_error_log = Instant::now();
    loop {
        let old_blockhash = *blockhash.read().unwrap();
        if exit_signal.load(Ordering::Relaxed) {
            break;
        }

        let new_slot = client.get_slot().unwrap();
        {
            slot.store(new_slot, Ordering::Release);
        }

        if let Some(new_blockhash) = get_new_latest_blockhash(client, &old_blockhash) {
            {
                *blockhash.write().unwrap() = new_blockhash;
            }
            blockhash_last_updated = Instant::now();
        } else {
            if blockhash_last_updated.elapsed().as_secs() > 120 {
                break;
            }
        }

        sleep(Duration::from_millis(100));
    }
}

fn seconds_since(dt: DateTime<Utc>) -> i64 {
    Utc::now().signed_duration_since(dt).num_seconds()
}

fn create_ask_bid_transaction(
    c: &PerpMarketCache,
    mango_account_pk: Pubkey,
    mango_account_signer: &Keypair,
    just_cancel: bool,
) -> Transaction {
    let mango_account_signer_pk = to_sp_pk(&mango_account_signer.pubkey());
    if just_cancel {
        let cancel_ix: Instruction = to_sdk_instruction(
            cancel_all_perp_orders(
                &c.mango_program_pk,
                &c.mango_group_pk,
                &mango_account_pk,
                &mango_account_signer_pk,
                &c.perp_market_pk,
                &c.perp_market.bids,
                &c.perp_market.asks,
                10,
            )
            .unwrap(),
        );

        Transaction::new_unsigned(Message::new(
            &[cancel_ix],
            Some(&mango_account_signer.pubkey()),
        ))
    } else {
        let offset = rand::random::<i8>() as i64;
        let spread = rand::random::<u8>() as i64;
        debug!(
            "price:{:?} price_quote_lots:{:?} order_base_lots:{:?} offset:{:?} spread:{:?}",
            c.price, c.price_quote_lots, c.order_base_lots, offset, spread
        );
        let cancel_ix: Instruction = to_sdk_instruction(
            cancel_all_perp_orders(
                &c.mango_program_pk,
                &c.mango_group_pk,
                &mango_account_pk,
                &mango_account_signer_pk,
                &c.perp_market_pk,
                &c.perp_market.bids,
                &c.perp_market.asks,
                10,
            )
            .unwrap(),
        );

        let place_bid_ix: Instruction = to_sdk_instruction(
            place_perp_order2(
                &c.mango_program_pk,
                &c.mango_group_pk,
                &mango_account_pk,
                &mango_account_signer_pk,
                &c.mango_cache_pk,
                &c.perp_market_pk,
                &c.perp_market.bids,
                &c.perp_market.asks,
                &c.perp_market.event_queue,
                None,
                &[],
                Side::Bid,
                c.price_quote_lots + offset - spread,
                c.order_base_lots,
                i64::MAX,
                1,
                mango::matching::OrderType::Limit,
                false,
                None,
                64,
                mango::matching::ExpiryType::Absolute,
            )
            .unwrap(),
        );

        let place_ask_ix: Instruction = to_sdk_instruction(
            place_perp_order2(
                &c.mango_program_pk,
                &c.mango_group_pk,
                &mango_account_pk,
                &mango_account_signer_pk,
                &c.mango_cache_pk,
                &c.perp_market_pk,
                &c.perp_market.bids,
                &c.perp_market.asks,
                &c.perp_market.event_queue,
                None,
                &[],
                Side::Ask,
                c.price_quote_lots + offset + spread,
                c.order_base_lots,
                i64::MAX,
                2,
                mango::matching::OrderType::Limit,
                false,
                None,
                64,
                mango::matching::ExpiryType::Absolute,
            )
            .unwrap(),
        );

        Transaction::new_unsigned(Message::new(
            &[cancel_ix, place_bid_ix, place_ask_ix],
            Some(&mango_account_signer.pubkey()),
        ))
    }
}

fn send_mm_transactions(
    quotes_per_second: u64,
    perp_market_caches: &Vec<PerpMarketCache>,
    tx_record_sx: &Sender<TransactionSendRecord>,
    tpu_client_pool: Arc<RotatingQueue<Arc<TpuClient>>>,
    mango_account_pk: Pubkey,
    mango_account_signer: &Keypair,
    blockhash: Arc<RwLock<Hash>>,
    slot: &AtomicU64,
) {
    let mango_account_signer_pk = to_sp_pk(&mango_account_signer.pubkey());
    // update quotes 2x per second
    for i in 0..quotes_per_second {
        for c in perp_market_caches.iter() {
            let mut tx =
                create_ask_bid_transaction(c, mango_account_pk, &mango_account_signer, i == 0);

            if let Ok(recent_blockhash) = blockhash.read() {
                tx.sign(&[mango_account_signer], *recent_blockhash);
            }
            let tpu_client = tpu_client_pool.get();
            tpu_client.send_transaction(&tx);
            let sent = tx_record_sx.send(TransactionSendRecord {
                signature: tx.signatures[0],
                sent_at: Utc::now(),
                sent_slot: slot.load(Ordering::Acquire),
                market_maker: mango_account_signer_pk,
                market: c.perp_market_pk,
            });
            if sent.is_err() {
                println!(
                    "sending error on channel : {}",
                    sent.err().unwrap().to_string()
                );
            }
        }
    }
}

fn send_mm_transactions_batched(
    txs_batch_size: usize,
    quotes_per_second: u64,
    perp_market_caches: &Vec<PerpMarketCache>,
    tx_record_sx: &Sender<TransactionSendRecord>,
    tpu_client_pool: Arc<RotatingQueue<Arc<TpuClient>>>,
    mango_account_pk: Pubkey,
    mango_account_signer: &Keypair,
    blockhash: Arc<RwLock<Hash>>,
    slot: &AtomicU64,
) {
    let mut transactions = Vec::<_>::with_capacity(txs_batch_size);

    let mango_account_signer_pk = to_sp_pk(&mango_account_signer.pubkey());
    // update quotes 2x per second
    for i in 0..quotes_per_second {
        for c in perp_market_caches.iter() {
            for j in 0..txs_batch_size {
                transactions.push(create_ask_bid_transaction(
                    c,
                    mango_account_pk,
                    &mango_account_signer,
                    i == 0 && j == 0,
                ));
            }

            if let Ok(recent_blockhash) = blockhash.read() {
                for tx in &mut transactions {
                    tx.sign(&[mango_account_signer], *recent_blockhash);
                }
            }
            let tpu_client = tpu_client_pool.get();
            if tpu_client
                .try_send_transaction_batch(&transactions)
                .is_err()
            {
                error!("Sending batch failed");
                continue;
            }

            for tx in &transactions {
                let sent = tx_record_sx.send(TransactionSendRecord {
                    signature: tx.signatures[0],
                    sent_at: Utc::now(),
                    sent_slot: slot.load(Ordering::Acquire),
                    market_maker: mango_account_signer_pk,
                    market: c.perp_market_pk,
                });
                if sent.is_err() {
                    error!(
                        "sending error on channel : {}",
                        sent.err().unwrap().to_string()
                    );
                }
            }
            transactions.clear();
        }
    }
}

#[derive(Clone, Serialize)]
struct BlockData {
    pub block_hash: String,
    pub block_slot: Slot,
    pub block_leader: String,
    pub total_transactions: u64,
    pub number_of_mm_transactions: u64,
    pub block_time: u64,
    pub cu_consumed: u64,
    pub mm_cu_consumed: u64,
}

fn aggregate_quic_messages(data: &Vec<QuicHandlerMessage>) -> String {
    let mut agg = HashMap::<String, Vec<u64>>::new();
    for d in data {
        match agg.get_mut(&d.message()) {
            Some(x) => {
                x.push(d.approximate_slot());
            }
            None => {
                agg.insert(d.message(), vec![d.approximate_slot()]);
            }
        }
    }
    agg.iter()
        .map(|(message, slots)| format!("{} ({:?}), ", message, slots))
        .reduce(|acc, e| acc + e.as_str())
        .unwrap()
}

fn confirmations_by_blocks(
    clients: RotatingQueue<Arc<RpcClient>>,
    start_slot: Slot,
    recv_limit: usize,
    tx_record_rx: Receiver<TransactionSendRecord>,
    tx_confirm_records: Arc<RwLock<Vec<TransactionRecord>>>,
    tx_block_data: Arc<RwLock<Vec<BlockData>>>,
    bidirectional_replies: Arc<RwLock<HashMap<Signature, Vec<QuicHandlerMessage>>>>,
) {
    let mut recv_until_confirm = recv_limit;
    let transaction_map = Arc::new(RwLock::new(
        HashMap::<Signature, TransactionSendRecord>::new(),
    ));

    while recv_until_confirm != 0 {
        match tx_record_rx.try_recv() {
            Ok(tx_record) => {
                let mut transaction_map = transaction_map.write().unwrap();
                debug!(
                    "add to queue len={} sig={}",
                    transaction_map.len() + 1,
                    tx_record.signature
                );
                transaction_map.insert(tx_record.signature, tx_record);
                recv_until_confirm -= 1;
            }
            Err(TryRecvError::Empty) => {
                debug!("channel emptied");
                sleep(Duration::from_millis(100));
            }
            Err(TryRecvError::Disconnected) => {
                {
                    info!("channel disconnected {}", recv_until_confirm);
                }
                debug!("channel disconnected");
                break; // still confirm remaining transctions
            }
        }
    }
    println!("finished mapping all the trasactions");
    sleep(Duration::from_secs(120));
    let commitment_confirmation = CommitmentConfig {
        commitment: CommitmentLevel::Confirmed,
    };
    let block_res = clients
        .get()
        .get_blocks_with_commitment(start_slot, None, commitment_confirmation)
        .unwrap();

    let nb_blocks = block_res.len();
    let nb_thread: usize = 16;
    println!("processing {} blocks", nb_blocks);

    let mut join_handles = Vec::new();
    for slot_batch in block_res
        .chunks(if nb_blocks > nb_thread {
            nb_blocks.div(nb_thread)
        } else {
            nb_blocks
        })
        .map(|x| x.to_vec())
    {
        let map = transaction_map.clone();
        let client = clients.get().clone();
        let tx_confirm_records = tx_confirm_records.clone();
        let tx_block_data = tx_block_data.clone();
        let bidirectional_replies = bidirectional_replies.clone();
        let joinble = Builder::new()
            .name("getting blocks and searching transactions".to_string())
            .spawn(move || {
                let bidirectional_replies = bidirectional_replies.read().unwrap();
                for slot in slot_batch {
                    // retry search for block 10 times
                    let mut block = None;
                    for _i in 0..=10 {
                        let block_res = client
                        .get_block_with_config(
                            slot,
                            RpcBlockConfig {
                                encoding: None,
                                transaction_details: None,
                                rewards: None,
                                commitment: Some(commitment_confirmation),
                                max_supported_transaction_version: None,
                            },
                        );

                        match block_res {
                            Ok(x) => {
                                block = Some(x);
                                break;
                            },
                            _=>{
                                // do nothing
                            }
                        }
                    }
                    let block = match block {
                        Some(x) => x,
                        None => continue,
                    };
                    let mut mm_transaction_count: u64 = 0;
                    let rewards = &block.rewards.unwrap();
                    let slot_leader =  match rewards
                            .iter()
                            .find(|r| r.reward_type == Some(RewardType::Fee))
                            {
                                Some(x) => x.pubkey.clone(),
                                None=> "".to_string(),
                            };

                    if let Some(transactions) = block.transactions {
                        let nb_transactions = transactions.len();
                        let mut cu_consumed : u64 = 0;
                        let mut mm_cu_consumed : u64 = 0;
                        for solana_transaction_status::EncodedTransactionWithStatusMeta {
                            transaction,
                            meta,
                            ..
                        } in transactions
                        {
                            if let solana_transaction_status::EncodedTransaction::Json(
                                transaction,
                            ) = transaction
                            {
                                for signature in transaction.signatures {
                                    let signature = Signature::from_str(&signature).unwrap();
                                    let transaction_record_op = {
                                        let map = map.read().unwrap();
                                        let rec = map.get(&signature);
                                        match rec {
                                            Some(x) => Some(x.clone()),
                                            None => None,
                                        }
                                    };
                                    // add CU in counter
                                    if let Some(meta) = &meta {
                                        match meta.compute_units_consumed {
                                            solana_transaction_status::option_serializer::OptionSerializer::Some(x) => {
                                                cu_consumed = cu_consumed.saturating_add(x);
                                            },
                                            _ => {},
                                        }
                                    }
                                    if let Some(transaction_record) = transaction_record_op {

                                        // add CU in counter
                                        if let Some(meta) = &meta {
                                            match meta.compute_units_consumed {
                                                solana_transaction_status::option_serializer::OptionSerializer::Some(x) => {
                                                    mm_cu_consumed = mm_cu_consumed.saturating_add(x);
                                                },
                                                _ => {},
                                            }
                                        }

                                        let mut lock = tx_confirm_records.write().unwrap();
                                        mm_transaction_count += 1;
                                        let bidir_message = bidirectional_replies.get(&signature).map(|x| aggregate_quic_messages(x));

                                        (*lock).push( TransactionRecord::new_confirmed(
                                            &transaction_record,
                                            slot,
                                            &meta,
                                            block.blockhash.clone(),
                                            slot_leader.clone(),
                                            bidir_message,
                                        ) );
                                    }

                                    map.write().unwrap().remove(&signature);
                                }
                            }
                        }
                        // push block data
                        {
                            let mut blockstats_writer = tx_block_data.write().unwrap();
                            blockstats_writer.push(BlockData {
                                block_hash: block.blockhash,
                                block_leader: slot_leader,
                                block_slot: slot,
                                block_time: if let Some(time) = block.block_time {
                                    time as u64
                                } else {
                                    0
                                },
                                number_of_mm_transactions: mm_transaction_count,
                                total_transactions: nb_transactions as u64,
                                cu_consumed: cu_consumed,
                                mm_cu_consumed: mm_cu_consumed,
                            })
                        }
                    }
                }
            })
            .unwrap();
        join_handles.push(joinble);
    }
    for handle in join_handles {
        handle.join().unwrap();
    }

    // process records where we had bidirectional replies
    let remaining_transactions = transaction_map.write().unwrap();
    let mut records = tx_confirm_records.write().unwrap();
    let bidirectional_replies = bidirectional_replies.read().unwrap();
    for x in remaining_transactions.iter() {
        let v = bidirectional_replies.get(x.0);
        match v {
            Some(v) => {
                records.push(TransactionRecord::new_errored(
                    x.1,
                    "timeout".to_string(),
                    Some(aggregate_quic_messages(v)),
                ));
            }
            None => {
                // timeout
                records.push(TransactionRecord::new_errored(
                    x.1,
                    "timeout".to_string(),
                    None,
                ));
            }
        }
    }
    // sort all blocks by slot and print info
    {
        let mut blockstats_writer = tx_block_data.write().unwrap();
        blockstats_writer.sort_by(|a, b| a.block_slot.partial_cmp(&b.block_slot).unwrap());
        for block_stat in blockstats_writer.iter() {
            info!(
                "block {} at slot {} contains {} transactions and consumed {} CUs",
                block_stat.block_hash,
                block_stat.block_slot,
                block_stat.total_transactions,
                block_stat.cu_consumed,
            );
        }
    }
}

fn write_transaction_data_into_csv(
    transaction_save_file: String,
    tx_confirm_records: Arc<RwLock<Vec<TransactionRecord>>>,
) {
    if transaction_save_file.is_empty() {
        return;
    }
    let mut writer = csv::Writer::from_path(transaction_save_file).unwrap();
    {
        let rd_lock = tx_confirm_records.read().unwrap();
        for confirm_record in rd_lock.iter() {
            writer.serialize(confirm_record).unwrap();
        }
    }
    writer.flush().unwrap();
}

fn write_block_data_into_csv(block_data_csv: String, tx_block_data: Arc<RwLock<Vec<BlockData>>>) {
    if block_data_csv.is_empty() {
        return;
    }
    let mut writer = csv::Writer::from_path(block_data_csv).unwrap();
    let data = tx_block_data.read().unwrap();

    for d in data.iter().filter(|x| x.number_of_mm_transactions > 0) {
        writer.serialize(d).unwrap();
    }
    writer.flush().unwrap();
}

fn main() {
    solana_logger::setup_with_default("solana=info");
    solana_metrics::set_panic_hook("bench-mango", /*version:*/ None);

    let matches = cli::build_args(solana_version::version!()).get_matches();
    let cli_config = cli::extract_args(&matches);

    let cli::Config {
        json_rpc_url,
        websocket_url,
        id,
        account_keys,
        mango_keys,
        duration,
        quotes_per_second,
        transaction_save_file,
        block_data_save_file,
        airdrop_accounts,
        mango_cluster,
        txs_batch_size,
        ..
    } = &cli_config;

    let transaction_save_file = transaction_save_file.clone();
    let block_data_save_file = block_data_save_file.clone();
    let airdrop_accounts = *airdrop_accounts;

    info!("Connecting to the cluster");

    let account_keys_json = fs::read_to_string(account_keys).expect("unable to read accounts file");
    let account_keys_parsed: Vec<AccountKeys> =
        serde_json::from_str(&account_keys_json).expect("accounts JSON was not well-formatted");

    let mango_keys_json = fs::read_to_string(mango_keys).expect("unable to read mango keys file");
    let mango_keys_parsed: MangoConfig =
        serde_json::from_str(&mango_keys_json).expect("mango JSON was not well-formatted");

    let mango_group_id = mango_cluster;
    let mango_group_config = mango_keys_parsed
        .groups
        .iter()
        .find(|g| g.name == *mango_group_id)
        .unwrap();

    let number_of_tpu_clients: usize = 1;
    let rpc_clients = RotatingQueue::<Arc<RpcClient>>::new(number_of_tpu_clients, || {
        Arc::new(RpcClient::new_with_commitment(
            json_rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ))
    });
    let start_slot = rpc_clients.get().get_slot().unwrap();
    let bidirectional_reply_handler = BidirectionalChannelHandler::new();

    let hashmap_bidirectional_replies = Arc::new(RwLock::new(HashMap::<
        Signature,
        Vec<QuicHandlerMessage>,
    >::new()));
    let _reply_print_jh = {
        let reply_handler = bidirectional_reply_handler.clone();
        let hashmap_bidirectional_replies = hashmap_bidirectional_replies.clone();
        std::thread::spawn(move || {
            let reply_handler = reply_handler;
            loop {
                let hash_map = hashmap_bidirectional_replies.clone();
                let res = reply_handler.reciever.recv();
                match res {
                    Ok(message) => {
                        let mut writer = hash_map.write().unwrap();
                        match writer.get_mut(&message.signature()) {
                            Some(x) => {
                                x.push(message);
                            }
                            None => {
                                writer.insert(message.signature(), vec![message]);
                            }
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        })
    };

    let tpu_client_pool = Arc::new(RotatingQueue::<Arc<TpuClient>>::new(
        number_of_tpu_clients,
        || {
            Arc::new(
                TpuClient::new_with_connection_cache(
                    rpc_clients.get().clone(),
                    &websocket_url,
                    solana_client::tpu_client::TpuClientConfig::default(),
                    Arc::new(ConnectionCache::new_with_replies_from_tpu(
                        4,
                        bidirectional_reply_handler.clone(),
                    )),
                )
                .unwrap(),
            )
        },
    ));

    info!(
        "accounts:{:?} markets:{:?} quotes_per_second:{:?} expected_tps:{:?} duration:{:?}",
        account_keys_parsed.len(),
        mango_group_config.perp_markets.len(),
        quotes_per_second,
        account_keys_parsed.len()
            * mango_group_config.perp_markets.len()
            * quotes_per_second.clone() as usize,
        duration
    );

    // continuosly fetch blockhash
    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        json_rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    ));
    let exit_signal = Arc::new(AtomicBool::new(false));
    let blockhash = Arc::new(RwLock::new(get_latest_blockhash(&rpc_client.clone())));
    let current_slot = Arc::new(AtomicU64::new(0));
    let blockhash_thread = {
        let exit_signal = exit_signal.clone();
        let blockhash = blockhash.clone();
        let client = rpc_client.clone();
        let current_slot = current_slot.clone();
        Builder::new()
            .name("solana-blockhash-poller".to_string())
            .spawn(move || {
                poll_blockhash_and_slot(
                    &exit_signal,
                    blockhash.clone(),
                    current_slot.as_ref(),
                    &client,
                );
            })
            .unwrap()
    };

    // fetch group
    let mango_group_pk = Pubkey::from_str(mango_group_config.public_key.as_str()).unwrap();
    let mango_group = load_from_rpc::<MangoGroup>(&rpc_client, &mango_group_pk);
    let mango_program_pk = Pubkey::from_str(mango_group_config.mango_program_id.as_str()).unwrap();
    let mango_cache_pk = Pubkey::from_str(mango_group.mango_cache.to_string().as_str()).unwrap();
    let mango_cache = load_from_rpc::<MangoCache>(&rpc_client, &mango_cache_pk);

    let perp_market_caches: Vec<PerpMarketCache> = mango_group_config
        .perp_markets
        .iter()
        .enumerate()
        .map(|(market_index, perp_maket_config)| {
            let perp_market_pk = Pubkey::from_str(perp_maket_config.public_key.as_str()).unwrap();
            let perp_market = load_from_rpc::<PerpMarket>(&rpc_client, &perp_market_pk);

            // fetch price
            let base_decimals = mango_group_config.tokens[market_index].decimals;
            let quote_decimals = mango_group_config.tokens[0].decimals;

            let base_unit = I80F48::from_num(10u64.pow(base_decimals as u32));
            let quote_unit = I80F48::from_num(10u64.pow(quote_decimals as u32));
            let price = mango_cache.price_cache[market_index].price;
            // println!(
            //     "market index {} price of  : {}",
            //     market_index, mango_cache.price_cache[market_index].price
            // );

            let price_quote_lots: i64 = price
                .mul(quote_unit)
                .mul(I80F48::from_num(perp_market.base_lot_size))
                .div(I80F48::from_num(perp_market.quote_lot_size))
                .div(base_unit)
                .to_num();
            let order_base_lots: i64 = base_unit
                .div(I80F48::from_num(perp_market.base_lot_size))
                .to_num();

            PerpMarketCache {
                order_base_lots,
                price,
                price_quote_lots,
                mango_program_pk,
                mango_group_pk,
                mango_cache_pk,
                perp_market_pk,
                perp_market,
            }
        })
        .collect();

    let (tx_record_sx, tx_record_rx) = channel::<TransactionSendRecord>();

    let mm_threads: Vec<JoinHandle<()>> = account_keys_parsed
        .iter()
        .map(|account_keys| {
            let _exit_signal = exit_signal.clone();
            // having a tpu client for each MM
            let tpu_client_pool = tpu_client_pool.clone();

            let blockhash = blockhash.clone();
            let current_slot = current_slot.clone();
            let duration = duration.clone();
            let quotes_per_second = quotes_per_second.clone();
            let perp_market_caches = perp_market_caches.clone();
            let mango_account_pk =
                Pubkey::from_str(account_keys.mango_account_pks[0].as_str()).unwrap();
            let mango_account_signer =
                Keypair::from_bytes(account_keys.secret_key.as_slice()).unwrap();

            if airdrop_accounts {
                println!("Transfering 1 SOL to {}", mango_account_signer.pubkey());
                let inx = solana_sdk::system_instruction::transfer(
                    &id.pubkey(),
                    &mango_account_signer.pubkey(),
                    LAMPORTS_PER_SOL,
                );

                let mut tx = Transaction::new_unsigned(Message::new(&[inx], Some(&id.pubkey())));

                if let Ok(recent_blockhash) = blockhash.read() {
                    tx.sign(&[id], *recent_blockhash);
                }
                rpc_client
                    .send_and_confirm_transaction_with_spinner(&tx)
                    .unwrap();
            }

            info!(
                "wallet:{:?} https://testnet.mango.markets/account?pubkey={:?}",
                mango_account_signer.pubkey(),
                mango_account_pk
            );
            //sleep(Duration::from_secs(10));

            let tx_record_sx = tx_record_sx.clone();
            let txs_batch_size: Option<usize> = *txs_batch_size;

            Builder::new()
                .name("solana-client-sender".to_string())
                .spawn(move || {
                    for _i in 0..duration.as_secs() {
                        let start = Instant::now();
                        // send market maker transactions
                        if let Some(txs_batch_size) = txs_batch_size.clone() {
                            send_mm_transactions_batched(
                                txs_batch_size,
                                quotes_per_second,
                                &perp_market_caches,
                                &tx_record_sx,
                                tpu_client_pool.clone(),
                                mango_account_pk,
                                &mango_account_signer,
                                blockhash.clone(),
                                current_slot.as_ref(),
                            );
                        } else {
                            send_mm_transactions(
                                quotes_per_second,
                                &perp_market_caches,
                                &tx_record_sx,
                                tpu_client_pool.clone(),
                                mango_account_pk,
                                &mango_account_signer,
                                blockhash.clone(),
                                current_slot.as_ref(),
                            );
                        }

                        let elapsed_millis: u64 = start.elapsed().as_millis() as u64;
                        if elapsed_millis < 950 {
                            // 50 ms is reserved for other stuff
                            sleep(Duration::from_millis(950 - elapsed_millis));
                        } else {
                            warn!(
                                "time taken to send transactions is greater than 1000ms {}",
                                elapsed_millis
                            );
                        }
                    }
                })
                .unwrap()
        })
        .collect();

    drop(tx_record_sx);
    let duration = duration.clone();
    let quotes_per_second = quotes_per_second.clone();
    let account_keys_parsed = account_keys_parsed.clone();
    let tx_confirm_records: Arc<RwLock<Vec<TransactionRecord>>> = Arc::new(RwLock::new(Vec::new()));
    let hashmap_bidirectional_replies = hashmap_bidirectional_replies.clone();

    let tx_block_data = Arc::new(RwLock::new(Vec::<BlockData>::new()));

    let confirmation_thread = Builder::new()
        .name("solana-client-sender".to_string())
        .spawn(move || {
            let recv_limit = account_keys_parsed.len()
                * perp_market_caches.len()
                * duration.as_secs() as usize
                * quotes_per_second as usize;

            //confirmation_by_querying_rpc(recv_limit, rpc_client.clone(), &tx_record_rx, tx_confirm_records.clone(), tx_timeout_records.clone());
            confirmations_by_blocks(
                rpc_clients,
                start_slot,
                recv_limit,
                tx_record_rx,
                tx_confirm_records.clone(),
                tx_block_data.clone(),
                hashmap_bidirectional_replies.clone(),
            );

            let records: Vec<TransactionRecord> = {
                let lock = tx_confirm_records.read().unwrap();
                (*lock).clone()
            };
            let confirmed_length = records.iter().filter(|x| !x.timeout).count();
            let timed_out = records.len() - confirmed_length;
            let total_signed = account_keys_parsed.len()
                * perp_market_caches.len()
                * duration.as_secs() as usize
                * quotes_per_second as usize;
            info!(
                "confirmed {} signatures of {} rate {}%",
                confirmed_length,
                total_signed,
                (confirmed_length * 100) / total_signed
            );
            let error_count = records
                .iter()
                .filter(|tx| !tx.timeout && !tx.error.is_empty())
                .count();
            info!(
                "errors counted {} rate {}%",
                error_count,
                (error_count as usize * 100) / total_signed
            );
            info!(
                "timeouts counted {} rate {}%",
                timed_out,
                (timed_out * 100) / total_signed
            );

            // let mut confirmation_times = confirmed
            //     .iter()
            //     .map(|r| {
            //         r.confirmed_at
            //             .signed_duration_since(r.sent_at)
            //             .num_milliseconds()
            //     })
            //     .collect::<Vec<_>>();
            // confirmation_times.sort();
            // info!(
            //     "confirmation times min={} max={} median={}",
            //     confirmation_times.first().unwrap(),
            //     confirmation_times.last().unwrap(),
            //     confirmation_times[confirmation_times.len() / 2]
            // );

            write_transaction_data_into_csv(transaction_save_file, tx_confirm_records);

            write_block_data_into_csv(block_data_save_file, tx_block_data);
        })
        .unwrap();

    for t in mm_threads {
        if let Err(err) = t.join() {
            error!("mm join failed with: {:?}", err);
        }
    }

    info!("joined all mm_threads");

    if let Err(err) = confirmation_thread.join() {
        error!("confirmation join fialed with: {:?}", err);
    }

    info!("joined confirmation thread");

    exit_signal.store(true, Ordering::Relaxed);

    if let Err(err) = blockhash_thread.join() {
        error!("blockhash join failed with: {:?}", err);
    }
}
