use axum::extract::ws::{Message, WebSocket};
// use futures_util::{SinkExt, StreamExt};
// use tokio_tungstenite::{accept_async, tungstenite::Message};

pub mod datatypes;
use crate::datatypes::{ClientMessage, ClientPayload, DbError, ServerMessage, ServerPayload};

// Authorisation layer for the database
mod auth;
use auth::account;
pub mod database;
mod sync;

use std::time::{SystemTime, UNIX_EPOCH};

/// Core function accepting a user attempting to connect to the server
pub async fn handle_connection(mut socket: WebSocket, db: database::MyDatabase) {
    println!("New WebSocket connection");

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
                let user_id: u32 = decoded.user_id;

                match decoded.payload {
                    ClientPayload::Login { name, password } => {
                        println!("Attempting login");
                        let login_attempt: Result<u32, DbError> =
                            account::login(name, password, db.clone()).await;
                        let login_detail = match login_attempt {
                            Ok(result) => {
                                println!("succeedded login");
                                let refresh_token =
                                    account::roll_refresh_token(result, db.clone()).await;
                                match refresh_token {
                                    Ok(refresh_token) => {
                                        let access_token =
                                            account::roll_access_token(refresh_token, db.clone())
                                                .await;
                                        match access_token {
                                            Ok(access_token) => Some((
                                                true,
                                                Some(refresh_token),
                                                Some(access_token),
                                            )),
                                            // TODO: Handle error
                                            Err(error) => {
                                                println!(
                                                    "Error happened at access token: {}",
                                                    error
                                                );
                                                None
                                            }
                                        }
                                    }
                                    // TODO: Handle error
                                    Err(_) => {
                                        println!("database error with refresh token");
                                        None
                                    }
                                }
                            }
                            Err(_) => Some((false, None, None)),
                        };
                        println!("got through login logic");
                        // TODO: handle the error of time going before UNIX_EPOCH, set time to 0?
                        let timestamp = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs() as u32;
                        let message = ServerMessage {
                            id,
                            timestamp,
                            payload: match login_detail {
                                Some(details) => {
                                    let (success, refresh_token, access_token) = details;
                                    ServerPayload::ConfirmLogin {
                                        success,
                                        refresh_token,
                                        access_token,
                                    }
                                }
                                None => ServerPayload::UnknownError,
                            },
                        };
                        let message_bytes = rmp_serde::to_vec(&message).unwrap();
                        let message_to_send = Message::binary(message_bytes);
                        // TODO: Error handling
                        let _send_result = socket.send(message_to_send).await;
                    }
                    ClientPayload::UpdateCheckbox { .. } => {}
                    ClientPayload::UpdateCheckboxDetails { .. } => {}
                    ClientPayload::RequestSync(timestamp) => {
                        let sync_result =
                            sync::sync_user(db.clone(), &mut socket, timestamp, user_id).await;
                        if let Err(error) = sync_result {
                            println!("an error occured while syncing: {}", error); // TODO: Handle error
                        }
                    }
                    ClientPayload::SetLowDataMode(..) => {}
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
