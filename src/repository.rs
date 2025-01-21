use std::collections::HashMap;
use std::vec;

use crate::db::DbError;

use super::db::DbPool;
use super::models::*;
use bigdecimal::BigDecimal;
use diesel::dsl::sum;
use diesel::prelude::*;
use diesel::PgConnection;  // Added for transaction support

#[derive(Clone)]
pub struct BlocksRepository {
    pub pool: DbPool,
}

impl BlocksRepository {
    // Transaction-aware version of insert
    pub fn insert_with_conn(&self, block: &Block, conn: &mut PgConnection) -> Result<Block, DbError> {
        use crate::schema::blocks::dsl::*;
        
        let new_block = diesel::insert_into(blocks)
            .values(block)
            .returning(Block::as_returning())
            .get_result(conn)?;
        Ok(new_block)
    }

    // Transaction-aware version of insert_batch
    pub fn insert_batch_with_conn(
        &self,
        blocks: &[Block],
        conn: &mut PgConnection
    ) -> Result<Vec<Block>, DbError> {
        use crate::schema::blocks::dsl::blocks as blocks_table;
        
        let inserted = diesel::insert_into(blocks_table)
            .values(blocks)
            .on_conflict_do_nothing()
            .returning(Block::as_returning())
            .get_results(conn)?;
        Ok(inserted)
    }

    // Existing methods remain the same
    pub fn find_by_hashes(&self, hashes: &[String]) -> Result<Vec<Block>, diesel::result::Error> {
        use crate::schema::blocks::dsl::{blocks, hash};
        let mut conn = self.pool.get().unwrap();
        let results = blocks
            .filter(hash.eq_any(hashes))
            .select(Block::as_select())
            .load::<Block>(&mut conn)?;
        Ok(results)
    }

    pub fn find_by_hash(
        &self,
        hash: &str,
        chain_id: i64,
    ) -> Result<Option<Block>, diesel::result::Error> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_column, hash as hash_column,
        };
        let mut conn = self.pool.get().unwrap();
        let result = blocks_table
            .filter(hash_column.eq(hash))
            .filter(chain_id_column.eq(chain_id))
            .select(Block::as_select())
            .first::<Block>(&mut conn)
            .optional()?;
        Ok(result)
    }

    pub fn find_by_height(&self, height: i64, chain_id: i64) -> Result<Option<Block>, DbError> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_column, height as height_column,
        };
        let mut conn = self.pool.get().unwrap();
        let result = blocks_table
            .filter(height_column.eq(height))
            .filter(chain_id_column.eq(chain_id))
            .select(Block::as_select())
            .first::<Block>(&mut conn)
            .optional()?;
        Ok(result)
    }

    pub fn find_by_range(
        &self,
        min_height: i64,
        max_height: i64,
        chain_id: i64,
    ) -> Result<Vec<Block>, DbError> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_column, height as height_column,
        };
        let mut conn = self.pool.get().unwrap();
        let results = blocks_table
            .filter(height_column.ge(min_height))
            .filter(height_column.le(max_height))
            .filter(chain_id_column.eq(chain_id))
            .select(Block::as_select())
            .order(height_column.desc())
            .load::<Block>(&mut conn)?;
        Ok(results)
    }

    pub fn find_min_max_height_blocks(
        &self,
        chain_id: i64,
    ) -> Result<(Option<Block>, Option<Block>), DbError> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_column, height,
        };
        let mut conn = self.pool.get().unwrap();
        let query = blocks_table.filter(chain_id_column.eq(chain_id));
        let min_block = query
            .order_by(height.asc())
            .select(Block::as_select())
            .first::<Block>(&mut conn)
            .optional()?;
        let max_block = query
            .order_by(height.desc())
            .select(Block::as_select())
            .first::<Block>(&mut conn)
            .optional()?;
        Ok((min_block, max_block))
    }

    pub fn count(&self, chain_id: i64) -> Result<i64, DbError> {
        use crate::schema::blocks::dsl::{blocks, chain_id as chain_id_col, height};
        use diesel::dsl::count;
        let mut conn = self.pool.get().unwrap();
        let count = blocks
            .select(count(height))
            .filter(chain_id_col.eq(chain_id))
            .first(&mut conn)?;
        Ok(count)
    }

    pub fn insert(&self, block: &Block) -> Result<Block, DbError> {
        use crate::schema::blocks::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let new_block = diesel::insert_into(blocks)
            .values(block)
            .returning(Block::as_returning())
            .get_result(&mut conn)?;
        Ok(new_block)
    }

    pub fn insert_batch(&self, blocks: &[Block]) -> Result<Vec<Block>, DbError> {
        use crate::schema::blocks::dsl::blocks as blocks_table;
        let mut conn = self.pool.get().unwrap();
        let inserted = diesel::insert_into(blocks_table)
            .values(blocks)
            .on_conflict_do_nothing()
            .returning(Block::as_returning())
            .get_results(&mut conn)?;
        Ok(inserted)
    }

    pub fn delete_all(&self) -> Result<usize, diesel::result::Error> {
        use crate::schema::blocks::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(blocks).execute(&mut conn)?;
        Ok(deleted)
    }

    #[allow(dead_code)]
    pub fn delete_one(&self, height: i64, chain_id: i64) -> Result<usize, DbError> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_col, height as height_col,
        };
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(
            blocks_table
                .filter(height_col.eq(height))
                .filter(chain_id_col.eq(chain_id)),
        )
        .execute(&mut conn)?;
        Ok(deleted)
    }

    pub fn delete_by_hash(&self, hash: &str, chain_id: i64) -> Result<usize, DbError> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_col, hash as hash_col,
        };
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(
            blocks_table
                .filter(hash_col.eq(hash))
                .filter(chain_id_col.eq(chain_id)),
        )
        .execute(&mut conn)?;
        Ok(deleted)
    }
}

#[derive(Clone)]
pub struct EventsRepository {
    pub pool: DbPool,
}

impl EventsRepository {
    // Transaction-aware version of insert_batch
    pub fn insert_batch_with_conn(
        &self,
        events: &[Event],
        conn: &mut PgConnection
    ) -> Result<usize, DbError> {
        use crate::schema::events::dsl::events as events_table;
        
        let mut inserted = 0;
        for chunk in events.chunks(1000) {
            inserted += diesel::insert_into(events_table)
                .values(chunk)
                .on_conflict_do_nothing()
                .execute(conn)?;
        }
        Ok(inserted)
    }

    // Existing methods remain unchanged
    #[allow(dead_code)]
    pub fn find_all(&self) -> Result<Vec<Event>, DbError> {
        use crate::schema::events::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let results = events.select(Event::as_select()).load::<Event>(&mut conn)?;
        Ok(results)
    }

    pub fn find_max_height(&self, chain_id: i64) -> Result<i64, DbError> {
        use crate::schema::events::dsl::{chain_id as chain_id_col, events, height as height_col};
        let mut conn = self.pool.get().unwrap();
        let max_height = events
            .filter(chain_id_col.eq(chain_id))
            .select(diesel::dsl::max(height_col))
            .first::<Option<i64>>(&mut conn)?;
        Ok(max_height.unwrap_or(0))
    }

    pub fn find_by_range(
        &self,
        min_height: i64,
        max_height: i64,
        chain_id: i64,
    ) -> Result<Vec<Event>, DbError> {
        use crate::schema::events::dsl::{chain_id as chain_id_col, events, height as height_col};
        let mut conn = self.pool.get().unwrap();
        let results = events
            .filter(chain_id_col.eq(chain_id))
            .filter(height_col.ge(min_height))
            .filter(height_col.le(max_height))
            .select(Event::as_select())
            .order(height_col.asc())
            .load::<Event>(&mut conn)?;
        Ok(results)
    }

    #[allow(dead_code)]
    pub fn insert(&self, event: &Event) -> Result<Event, DbError> {
        use crate::schema::events::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let new_event = diesel::insert_into(events)
            .values(event)
            .on_conflict((block, idx, request_key))
            .do_update()
            .set(event)
            .returning(Event::as_returning())
            .get_result(&mut conn)?;
        Ok(new_event)
    }

    pub fn insert_batch(&self, events: &[Event]) -> Result<usize, diesel::result::Error> {
        use crate::schema::events::dsl::events as events_table;
        let mut inserted = 0;
        let mut conn = self.pool.get().unwrap();
        for chunk in events.chunks(1000) {
            inserted += diesel::insert_into(events_table)
                .values(chunk)
                .on_conflict_do_nothing()
                .execute(&mut conn)?;
        }
        Ok(inserted)
    }

    #[allow(dead_code)]
    pub fn delete_all(&self) -> Result<usize, DbError> {
        use crate::schema::events::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(events).execute(&mut conn)?;
        Ok(deleted)
    }

    #[allow(dead_code)]
    pub fn delete_one(&self, block: &str, idx: i64, request_key: &str) -> Result<usize, DbError> {
        use crate::schema::events::dsl::{
            block as block_col, events, idx as idx_col, request_key as request_key_col,
        };
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(
            events
                .filter(block_col.eq(block))
                .filter(idx_col.eq(idx))
                .filter(request_key_col.eq(request_key)),
        )
        .execute(&mut conn)?;
        Ok(deleted)
    }

    pub fn delete_all_by_block(&self, hash: &str) -> Result<usize, DbError> {
        use crate::schema::events::dsl::{block as block_col, events};
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(events.filter(block_col.eq(hash))).execute(&mut conn)?;
        Ok(deleted)
    }
}

#[derive(Clone)]
pub struct TransactionsRepository {
    pub pool: DbPool,
}

impl TransactionsRepository {
    // Transaction-aware version of insert_batch
    pub fn insert_batch_with_conn(
        &self,
        transactions: &[Transaction],
        conn: &mut PgConnection
    ) -> Result<usize, DbError> {
        use crate::schema::transactions::dsl::transactions as transactions_table;
        
        let mut inserted = 0;
        for chunk in transactions.chunks(1000) {
            inserted += diesel::insert_into(transactions_table)
                .values(chunk)
                .on_conflict_do_nothing()
                .execute(conn)?;
        }
        Ok(inserted)
    }

    // Existing methods remain unchanged
    #[allow(dead_code)]
    pub fn find_all(&self) -> Result<Vec<Transaction>, DbError> {
        use crate::schema::transactions::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let results = transactions
            .select(Transaction::as_select())
            .load::<Transaction>(&mut conn)?;
        Ok(results)
    }

    #[allow(dead_code)]
    pub fn find_by_request_key(
        &self,
        request_keys: &Vec<String>,
    ) -> Result<Vec<Transaction>, DbError> {
        use crate::schema::transactions::dsl::{
            request_key as request_key_column, transactions as transactions_table,
        };
        let mut conn = self.pool.get().unwrap();
        let result = transactions_table
            .filter(request_key_column.eq_any(request_keys))
            .select(Transaction::as_select())
            .load(&mut conn)?;
        Ok(result)
    }

    #[allow(dead_code)]
    pub fn find_all_related(
        &self,
        request_keys: &Vec<String>,
    ) -> Result<HashMap<String, Vec<Transaction>>, DbError> {
        match self.find_by_request_key(request_keys) {
            Ok(transactions) => {
                let mut result = HashMap::new();
                for tx in transactions.iter() {
                    if tx.pact_id.is_some() {
                        match self.find_by_pact_id(&vec![tx.pact_id.clone().unwrap()]) {
                            Ok(multi_step_txs) => {
                                result.insert(tx.request_key.clone(), multi_step_txs);
                            }