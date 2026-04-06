use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    Login {name: String, password: String},
    UpdateCheckbox {map: i32, id: i32, checked: bool},
    UpdateCheckboxDetails {map: i32, id: i32, name: String},
    SetLowDataMode (bool)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerMessage {
    Confirm (bool)
}