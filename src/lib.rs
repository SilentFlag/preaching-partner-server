use axum::extract::ws::{Message, WebSocket};
// use futures_util::{SinkExt, StreamExt};
// use tokio_tungstenite::{accept_async, tungstenite::Message};

pub mod datatypes;
use crate::datatypes::{ClientMessage, ClientPayload};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast::{Receiver, Sender};

// Authorisation layer for the database
pub mod auth;
pub mod database;
pub mod events;
mod sync;

/// Core function accepting a user attempting to connect to the server
pub async fn handle_connection(
    mut socket: WebSocket,
    db: database::MyDatabase,
    event_tx: Sender<datatypes::ServerEvent>,
) {
    println!("New WebSocket connection");
    // TODO: use this to confirm session
    let _user_id: Option<u32> = None;
    let mut event_rx: Receiver<datatypes::ServerEvent> = event_tx.subscribe();

    // TODO: RUSTLS ENCRYPTION

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                match event {
                    datatypes::ServerEvent::AddressCompleted { id, checked } => {
                        println!("recieved event: AddressCompleted {{ id: {}, checked: {} }}", id, checked);
                        let timestamp = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs() as u32;
                        let message = datatypes::ServerMessage {
                            id,
                            timestamp,
                            payload: datatypes::ServerPayload::AddressCompleted { id, checked },
                        };
                        let message_bytes = rmp_serde::to_vec(&message).expect("failed to encode sync message"); // TODO: handle error
                        let message_to_send = Message::binary(message_bytes);
                        let _send_result = socket.send(message_to_send).await; // TODO: Handle error
                    }
                }
            }
            Some(msg) = socket.recv() => {
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
                        let _id: u32 = decoded.id;
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

                                events::broadcast_event(event_tx.clone(), datatypes::ServerEvent::AddressCompleted { id: address_id, checked }); // TODO: Broadcast event to all connected clients

                                // TODO: Remove this when the broadcast_event function is implemented
                                // This sends the event to the client that completed the address, but not to other clients
                                // let timestamp = SystemTime::now()
                                //     .duration_since(UNIX_EPOCH)
                                //     .expect("Time went backwards")
                                //     .as_secs() as u32;
                                // let message = datatypes::ServerMessage {
                                //     id,
                                //     timestamp,
                                //     payload: datatypes::ServerPayload::AddressCompleted {
                                //         id: address_id,
                                //         checked,
                                //     },
                                // };
                                // let message_bytes = rmp_serde::to_vec(&message).expect("failed to encode sync message");
                                // let message_to_send = Message::binary(message_bytes);
                                // let _send_result = socket.send(message_to_send).await;
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
        }
    }
    println!("Connection closed");
}
