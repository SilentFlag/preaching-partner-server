// TODO: Documentation

use rand::rngs::SysError;
use serde::{Deserialize, Serialize};
use std::fmt;
// use tokio::sync::{mpsc, oneshot, broadcast};

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientMessage {
    pub id: u32,
    pub access_token: Option<[u8; 32]>,
    pub payload: ClientPayload,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientPayload {
    Login { name: String, password: String },
    RequestAccessToken([u8; 32]),
    RequestSync(u32),
    CompleteAddress { id: u32, checked: bool },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ServerMessage {
    pub id: u32,
    pub timestamp: u32,
    pub payload: ServerPayload,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerPayload {
    ConfirmLogin {
        success: bool,
        refresh_token: Option<[u8; 32]>,
        access_token: Option<[u8; 32]>,
    },
    SyncInformation(SyncInformation),
    NewAccessToken([u8; 32]),
    AddressCompleted { id: u32, checked: bool },
    UnknownError,
}

// Sync info
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MapDetails {
    pub id: u32,
    pub name: String,
    pub image_name: String,
    pub assignee: u32,
    pub assigner: u32,
    pub image: Option<Vec<u8>>,
    pub category: u32,
    pub deleted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AddressTags {
    DoNotCall,
    NoJunkMail,
    Custom(String),
}

// TODO: use
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AddressDetails {
    pub id: u32,
    pub street_id: u32,
    pub number: String,
    pub tags: Vec<AddressTags>,
    pub visited: bool,
    pub deleted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StreetDetails {
    pub id: u32,
    pub map_id: u32,
    pub name: String,
    pub deleted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CongDetails {
    pub cong_id: u32,
    pub cong_name: String,
    pub remove: bool,
    pub updated: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CategoryDetails {
    pub id: u32,
    pub name: String,
    pub prefix: String,
    pub congregation: u32,
    pub updated: u32,
    pub remove: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupDetails {
    pub id: u32,
    pub name: String,
    pub cong: u32,
    pub elder: u32,
    pub updated: u32,
    pub group_deleted: bool, // TODO: Condense into one deleted variable
    pub pair_deleted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserPublicDetails {
    pub id: u32,
    pub name: String,
    pub deleted: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncInformation {
    pub congregations: Vec<CongDetails>,
    pub categories: Vec<CategoryDetails>,
    pub service_groups: Vec<GroupDetails>,
    pub users: Vec<UserPublicDetails>,
    pub maps: Vec<MapDetails>,
    pub streets: Vec<StreetDetails>,
    pub addresses: Vec<AddressDetails>,
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
    AddressFailure(AddressError),
    UnknownError(sqlx::Error),
    Error,
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
            DbError::AddressFailure(error) => {
                write!(f, "something went wrong with the addresses: {}", error)
            }
            DbError::UnknownError(error) => write!(f, "an unknown error occured: {}", error),
            DbError::Error => write!(f, "a dberror::error error occured"),
        }
    }
}

#[derive(Debug)]
pub enum AddressError {
    SqlxError(sqlx::Error),
    DeserialiseError(rmp_serde::decode::Error),
}

impl From<sqlx::Error> for AddressError {
    fn from(err: sqlx::Error) -> Self {
        AddressError::SqlxError(err)
    }
}

impl From<rmp_serde::decode::Error> for AddressError {
    fn from(err: rmp_serde::decode::Error) -> Self {
        AddressError::DeserialiseError(err)
    }
}

impl fmt::Display for AddressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AddressError::DeserialiseError(error) => {
                write!(f, "something went wrong deserialising the tags: {}", error)
            }
            AddressError::SqlxError(error) => {
                write!(f, "something went wrong with sqlx: {}", error)
            }
        }
    }
}
