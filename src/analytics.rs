// analytics.rs
use chrono::{Duration, NaiveDateTime};
use diesel::prelude::*;
use bigdecimal::BigDecimal;
use std::collections::HashMap;

use crate::db::DbError;
use crate::models::*;

/// Represents a period during which a token was held
pub struct HoldingPeriod {
    pub address: String,
    pub token_id: String,
    pub acquisition_time: NaiveDateTime,
    pub disposal_time: Option<NaiveDateTime>,
    pub acquisition_amount: BigDecimal,
    pub current_amount: BigDecimal,
}

/// Represents a connection between two wallets
pub struct WalletConnection {
    pub from_address: String,
    pub to_address: String,
    pub total_transfers: i64,
    pub total_amount: BigDecimal,
    pub last_transfer_time: NaiveDateTime,
}

/// Represents transaction activity in a time period
pub struct ActivityPeriod {
    pub start_time: NaiveDateTime,
    pub end_time: NaiveDateTime,
    pub transaction_count: i64,
    pub total_amount: BigDecimal,
}