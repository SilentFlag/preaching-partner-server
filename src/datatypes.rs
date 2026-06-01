use rand::rngs::SysError;
use serde::{Deserialize, Serialize};
use std::fmt;
// use tokio::sync::{mpsc, oneshot, broadcast};

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientMessage {
    pub id: u32,
    pub payload: ClientPayload,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientPayload {
    Login { name: String, password: String },
    UpdateCheckbox { map: i32, id: i32, checked: bool },
    UpdateCheckboxDetails { map: i32, id: i32, name: String },
    SetLowDataMode(bool),
    RequestSync(u64),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ServerMessage {
    pub id: u32,
    pub timestamp: u64,
    pub payload: ServerPayload,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerPayload {
    Confirm(bool),
    ConfirmLogin {
        success: bool,
        refresh_token: Option<[u8; 32]>,
        access_token: Option<[u8; 32]>,
    },
    MapImage {
        image_name: String,
        image: Vec<u8>,
        assignee: u32,
        assigner: u32,
        category: u32,
    },
    SyncCong {
        cong_id: u32,
        cong_name: String,
        remove: bool,
    },
    SyncGroup {
        id: u32,
        name: String,
        cong: u32,
        elder: u32,
        updated: u64,
        deleted: bool,
    },
    SyncComplete,
    UnknownError,
}

// The following structs are for syncing
pub struct MapDetails {
    pub image_name: String,
    pub assignee: u32,
    pub assigner: u32,
    pub category: u32,
}

pub struct CongDetails {
    pub cong_id: u32,
    pub cong_name: String,
    pub timestamp: u64,
    pub remove: bool,
    pub updated: u64,
}

pub struct CategoryDetails {
    pub id: u32,
    pub name: String,
    pub prefix: String,
    pub congregation: u32,
    pub updated: u64,
}

pub struct GroupDetails {
    pub id: u32,
    pub name: String,
    pub cong: u32,
    pub elder: u32,
    pub updated: u64,
    pub group_deleted: bool,
    pub pair_deleted: bool,
}

pub struct UserPublicDetails {
    pub id: u32,
    pub name: String,
    pub updated: u64,
}

/// All errors relating to MyDatabase
#[derive(Debug)]
pub enum DbError {
    InvalidLocation(sqlx::Error),
    InvalidToken(u32),
    InvalidRow(sqlx::Error),
    ConnectionFailure(sqlx::Error),
    QueryFailure(sqlx::Error),
    TokenRngFailure(SysError),
    UnknownError(sqlx::Error),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::InvalidLocation(error) => write!(f, "db not found: {}", error),
            DbError::InvalidToken(error) => {
                write!(f, "token returned an invalid number of users: {}", error)
            }
            DbError::InvalidRow(error) => {
                write!(
                    f,
                    "an invalid row was passed to a parsing function: {}",
                    error
                )
            }
            DbError::ConnectionFailure(error) => write!(f, "connection to db failed: {}", error),
            DbError::QueryFailure(error) => write!(f, "a query failed to run: {}", error),
            DbError::TokenRngFailure(error) => write!(f, "a token failed to generate: {}", error),
            DbError::UnknownError(error) => write!(f, "an unknown error occured: {}", error),
        }
    }
}
