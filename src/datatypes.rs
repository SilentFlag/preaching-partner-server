use serde::{Deserialize, Serialize};
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
    SyncComplete,
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
