// This file will contain helper functions for events such as when an address is checked or when a map is assigned to a user/group. These events will be sent to all applicable connected clients
use crate::datatypes;
use tokio::sync::broadcast::Sender;

pub fn broadcast_event(event_tx: Sender<datatypes::ServerEvent>, event: datatypes::ServerEvent) {
    // TODO: Handle errors
    let _ = event_tx.send(event).unwrap();
    println!("Event broadcasted");
}

// More functions will be needed when events applicable to a single user or group are implemented, such as when a map is assigned to a user/group
