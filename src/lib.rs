use axum::extract::ws::{Message, WebSocket};
// use futures_util::{SinkExt, StreamExt};
// use tokio_tungstenite::{accept_async, tungstenite::Message};

pub mod datatypes;
use crate::datatypes::{ClientMessage, ClientPayload};
use std::time::{SystemTime, UNIX_EPOCH};

// Authorisation layer for the database
pub mod auth;
pub mod database;
pub mod events;
mod sync;

/// Core function accepting a user attempting to connect to the server
pub async fn handle_connection(mut socket: WebSocket, db: database::MyDatabase) {
    println!("New WebSocket connection");
    // TODO: use this to confirm session
    let _user_id: Option<u32> = None;

    // TODO: RUSTLS ENCRYPTION

    while let Some(msg) = socket.recv().await {
        // TODO: handle corrupt messages
        let msg = match msg {
            Ok(msg) => msg,
            Err(error) => {
                eprintln!(
                    "Something went wrong with the recieved message: {:?}",
                    error
                );
                continue;
            }
        };
        match msg {
            Message::Binary(bin) => {
                let decoded: ClientMessage = rmp_serde::from_slice(&bin).unwrap();
                let id: u32 = decoded.id;
                let _user_token: Option<[u8; 32]> = decoded.access_token;
                let user_id = 1; // TODO: Get user id from token

                match decoded.payload {
                    ClientPayload::RequestSync(timestamp) => {
                        let sync_result =
                            sync::sync_user(db.clone(), &mut socket, timestamp, user_id).await;
                        if let Err(error) = sync_result {
                            println!("an error occured while syncing: {}", error); // TODO: Handle error
                        }
                    }
                    ClientPayload::CompleteAddress { id: address_id, checked } => {
                        let result = &db.complete_address(address_id, checked).await;
                        if let Err(error) = result {
                            println!("an error occured while completing address: {}", error); // TODO: Handle error
                        }

                        events::broadcast_event(); // TODO: Broadcast event to all connected clients

                        // TODO: Remove this when the broadcast_event function is implemented
                        // This sends the event to the client that completed the address, but not to other clients
                        let timestamp = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs() as u32;
                        let message = datatypes::ServerMessage {
                            id,
                            timestamp,
                            payload: datatypes::ServerPayload::AddressCompleted {
                                id: address_id,
                                checked,
                            },
                        };
                        let message_bytes = rmp_serde::to_vec(&message).expect("failed to encode sync message");
                        let message_to_send = Message::binary(message_bytes);
                        let _send_result = socket.send(message_to_send).await;
                    }
                    _ => {
                        // Invalid request (eg Login)
                    }
                }
            }
            Message::Close(_) => {
                println!("Client disconnected");
                break;
            }
            _ => {}
        }
    }

    println!("Connection closed");
}
