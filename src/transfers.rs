#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::models::Block;
    use crate::repository::BlocksRepository;
    use bigdecimal::BigDecimal;
    use chrono::Utc;
    use serial_test::serial;

    fn make_block(chain_id: i64, height: i64, hash: String) -> Block {
        Block {
            chain_id,
            hash,
            height,
            parent: "parent".to_string(),
            weight: BigDecimal::from(0),
            creation_time: Utc::now().naive_utc(),
            epoch: Utc::now().naive_utc(),
            flags: BigDecimal::from(0),
            miner: "miner".to_string(),
            nonce: BigDecimal::from(0),
            payload: "payload".to_string(),
            pow_hash: "".to_string(),
            predicate: "predicate".to_string(),
            target: BigDecimal::from(1),
        }
    }

    fn make_transfer_event(
        block: String,
        height: i64,
        idx: i64,
        chain_id: i64,
        from: String,
        to: String,
        amount: f64,
    ) -> Event {
        use rand::distributions::{Alphanumeric, DistString};
        Event {
            block: block.clone(),
            chain_id,
            height,
            idx,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!([from, to, amount]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: Alphanumeric.sample_string(&mut rand::thread_rng(), 16),
            pact_id: None,
        }
    }

    #[test]
    #[serial]
    fn test_transfers_backfill() {
        dotenvy::from_filename(".env.test").ok();
        let pool = db::initialize_db_pool();
        let blocks_repository = BlocksRepository { pool: pool.clone() };
        let events_repository = EventsRepository { pool: pool.clone() };
        let transfers_repository = TransfersRepository { pool: pool.clone() };
        blocks_repository
            .insert_batch(&[
                make_block(0, 0, "block-0".to_string()),
                make_block(0, 1, "block-1".to_string()),
                make_block(0, 2, "block-2".to_string()),
            ])
            .unwrap();
        events_repository
            .insert_batch(&[
                make_transfer_event(
                    "block-0".to_string(),
                    0,
                    0,
                    0,
                    "bob".to_string(),
                    "alice".to_string(),
                    100.1,
                ),
                make_transfer_event(
                    "block-0".to_string(),
                    0,
                    1,
                    0,
                    "alice".to_string(),
                    "bob".to_string(),
                    10.0,
                ),
                make_transfer_event(
                    "block-2".to_string(),
                    2,
                    0,
                    0,
                    "alice".to_string(),
                    "bob".to_string(),
                    10.1,
                ),
                make_transfer_event(
                    "block-2".to_string(),
                    2,
                    1,
                    0,
                    "alice".to_string(),
                    "bob".to_string(),
                    5.5,
                ),
            ])
            .unwrap();
        backfill_chain(
            0,
            1,
            &events_repository,
            &blocks_repository,
            &transfers_repository,
            None,
        )
        .unwrap();

        let bob_incoming_transfers = transfers_repository
            .find(None, Some(String::from("bob")), None)
            .unwrap();
        assert!(bob_incoming_transfers.len() == 3);
        let alice_incoming_transfers = transfers_repository
            .find(None, Some(String::from("alice")), None)
            .unwrap();
        assert!(alice_incoming_transfers.len() == 1);

        events_repository.delete_all().unwrap();
        transfers_repository.delete_all().unwrap();
        blocks_repository.delete_all().unwrap();
    }

    #[test]
    fn test_make_transfer() {
        let event = Event {
            block: "block-hash".to_string(),
            chain_id: 0,
            height: 0,
            idx: 0,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!(["bob", "alice", 100.12324354665567]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: "request-key".to_string(),
            pact_id: None,
        };
        let block = make_block(0, 0, "hash".to_string());
        let transfer = make_transfer(&event, &block);
        assert_eq!(
            transfer,
            Transfer {
                amount: BigDecimal::from_str("100.12324354665567").unwrap(),
                block: "block-hash".to_string(),
                chain_id: 0,
                creation_time: NaiveDateTime::from_timestamp_millis(
                    block.creation_time.timestamp_millis()
                )
                .unwrap(),
                from_account: "bob".to_string(),
                height: 0,
                idx: 0,
                module_hash: "module-hash".to_string(),
                module_name: "coin".to_string(),
                request_key: "request-key".to_string(),
                to_account: "alice".to_string(),
                pact_id: None
            }
        );

        let no_sender_event = Event {
            params: serde_json::json!(["", "alice", 10]),
            ..event.clone()
        };
        let transfer = make_transfer(&no_sender_event, &block);
        assert_eq!(
            transfer,
            Transfer {
                amount: BigDecimal::from_str("10").unwrap(),
                block: "block-hash".to_string(),
                chain_id: 0,
                creation_time: NaiveDateTime::from_timestamp_millis(
                    block.creation_time.timestamp_millis()
                )
                .unwrap(),
                from_account: "".to_string(),
                height: 0,
                idx: 0,
                module_hash: "module-hash".to_string(),
                module_name: "coin".to_string(),
                request_key: "request-key".to_string(),
                to_account: "alice".to_string(),
                pact_id: None
            }
        );
        let no_receiver_event = Event {
            params: serde_json::json!(["bob", "", 10]),
            ..event
        };
        let transfer = make_transfer(&no_receiver_event, &block);
        assert_eq!(
            transfer,
            Transfer {
                amount: BigDecimal::from_str("10").unwrap(),
                block: "block-hash".to_string(),
                chain_id: 0,
                creation_time: NaiveDateTime::from_timestamp_millis(
                    block.creation_time.timestamp_millis()
                )
                .unwrap(),
                from_account: "bob".to_string(),
                height: 0,
                idx: 0,
                module_hash: "module-hash".to_string(),
                module_name: "coin".to_string(),
                request_key: "request-key".to_string(),
                to_account: "".to_string(),
                pact_id: None
            }
        );
    }

    #[test]
    fn test_parse_transfer_event_decimal() {
        let event = Event {
            block: "block-hash".to_string(),
            chain_id: 0,
            height: 0,
            idx: 0,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!(["bob", "alice", {"decimal": "22.230409400000000000000000"}]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: "request-key".to_string(),
            pact_id: None,
        };
        let block = make_block(0, 0, "hash".to_string());
        let transfer = make_transfer(&event, &block);
        assert!(transfer.amount == BigDecimal::from_str("22.230409400000000000000000").unwrap());

        let event = Event {
            block: "block-hash".to_string(),
            chain_id: 0,
            height: 0,
            idx: 0,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!(["bob", "alice", {"int": 1}]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: "request-key".to_string(),
            pact_id: None,
        };
        let transfer = make_transfer(&event, &block);
        assert!(transfer.amount == BigDecimal::from(1));
    }

    #[test]
    fn test_make_transfer_when_event_has_string_as_amount() {
        let event = Event {
            block: "block-hash".to_string(),
            chain_id: 0,
            height: 0,
            idx: 0,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!(["bob", "alice", "wrong-amount"]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: "request-key".to_string(),
            pact_id: None,
        };
        let block = make_block(0, 0, "hash".to_string());
        let transfer = make_transfer(&event, &block);
        assert!(transfer.amount == BigDecimal::from(0));
    }

    #[test]
    fn test_is_balance_transfer() {
        let event = Event {
            block: "block-hash".to_string(),
            chain_id: 0,
            height: 0,
            idx: 0,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!(["bob", "alice", 100.1]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: "request-key".to_string(),
            pact_id: None,
        };
        assert!(is_balance_transfer(&event));
        let event = Event {
            name: "NOT_TRANSFER".to_string(),
            ..event
        };
        assert!(is_balance_transfer(&event) == false);
    }
}