use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{accept_async, tungstenite::Message};

pub mod datatypes;
use crate::datatypes::{ClientMessage, ClientPayload, ServerMessage, ServerPayload};

// Authorisation layer for the database
mod auth;
use auth::account;
mod database;
mod sync;

use std::time::{SystemTime, UNIX_EPOCH};

/// Core function accepting a user attempting to connect to the server
pub async fn handle_connection(stream: tokio::net::TcpStream, db: database::MyDatabase) {
    let ws_stream = accept_async(stream)
        .await
        .expect("Failed to accept WebSocket");

    println!("New WebSocket connection");

    let (mut write, mut read) = ws_stream.split();

    // TODO: RUSTLS ENCRYPTION

    // Example Query
    // let rows = sqlx::query("INSERT INTO users(firstname, lastname) VALUES ('my', 'name')").execute(&db).await;

    // Handle incoming requests

    while let Some(msg) = read.next().await {
        // TODO: handle corrupt messages
        let msg = msg.unwrap();
        match msg {
            Message::Binary(bin) => {
                let decoded: ClientMessage = rmp_serde::from_slice(&bin).unwrap();
                let id: u32 = decoded.id;

                match decoded.payload {
                    ClientPayload::Login { name, password } => {
                        let login_attempt: Result<u32, bool> =
                            account::login(name, password, db.clone()).await;
                        let (success, refresh_token, access_token) = match login_attempt {
                            Ok(result) => {
                                let refresh_token =
                                    account::roll_refresh_token(result, db.clone()).await;
                                let access_token =
                                    account::roll_access_token(refresh_token, db.clone()).await;
                                (true, Some(refresh_token), access_token)
                            }
                            Err(_) => (false, None, None),
                        };
                        // TODO: handle the error of time going before UNIX_EPOCH, set time to 0?
                        let current_time = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_millis() as u64;
                        let confirm_message = ServerMessage {
                            id: id,
                            timestamp: current_time,
                            payload: ServerPayload::ConfirmLogin {
                                success,
                                refresh_token,
                                access_token,
                            },
                        };
                        let message_bytes = rmp_serde::to_vec(&confirm_message).unwrap();
                        let _ = write
                            .send(tokio_tungstenite::tungstenite::Message::binary(
                                message_bytes,
                            ))
                            .await;
                    }
                    ClientPayload::UpdateCheckbox { .. } => {}
                    ClientPayload::UpdateCheckboxDetails { .. } => {}
                    ClientPayload::RequestSync(time) => {
                        sync::sync_user(db.clone(), &mut write, time, id).await;
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
