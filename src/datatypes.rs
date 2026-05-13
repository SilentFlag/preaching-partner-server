use serde::{Deserialize, Serialize};
// use tokio::sync::{mpsc, oneshot, broadcast};

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientAction {
    CheckBox,
    AssignMap,
    AddUser,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientMessage {
    pub id: u32,
    pub action_list: Vec<ClientAction>,
    pub name: String,
    pub token: Vec<u8>,
    pub payload: Vec<u8>,
}
