use serde::{Serialize, Deserialize};
// use tokio::sync::{mpsc, oneshot, broadcast};

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientMessage {
    pub id: u32,
    pub payload: ClientPayload,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientPayload {
    Login {name: String, password: String},
    UpdateCheckbox {map: i32, id: i32, checked: bool},
    UpdateCheckboxDetails {map: i32, id: i32, name: String},
    SetLowDataMode (bool),
    RequestSync(u64)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ServerMessage {
    pub id: u32,
    pub timestamp: u64,
    pub payload: ServerPayload,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerPayload {
    Confirm (bool),
    ConfirmLogin {success: bool, refresh_token: Option<[u8; 32]>, access_token: Option<[u8; 32]>},
    MapImage(Vec<u8>),
}

// pub struct WsState {
//     pub request_tx: mpsc::Sender<WsRequest>,
//     pub event_tx: broadcast::Sender<WsEvent>,
// }

// pub struct WsRequest {
//     pub payload: String,
//     pub response_tx: oneshot::Sender<String>,
// }

// #[derive(Clone, Debug)]
// pub struct WsEvent {
//     pub payload: String,
// }
