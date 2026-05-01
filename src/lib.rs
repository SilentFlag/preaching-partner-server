use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};

pub mod datatypes;
use crate::datatypes::{ClientMessage, ClientPayload, ServerPayload, ServerMessage};

mod account;
use account::{login, roll_refresh_token, roll_access_token};

use std::time::{SystemTime, UNIX_EPOCH};

use std::fs;


/// Core function accepting a user attempting to connect to the server
pub async fn handle_connection(stream: tokio::net::TcpStream, db: sqlx::Pool<sqlx::Sqlite>) {
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
            Message::Text(..) => {
                // Incoming text messages, these are unexpected.
            }
            Message::Binary(bin) => {
                // Handle binary messages if needed
                let decoded: ClientMessage = rmp_serde::from_slice(&bin).unwrap();
                let id: u32 = decoded.id;

                match decoded.payload {
                    ClientPayload::Login {name, password} => {
                        let login_attempt: Result<u32, bool> = login(name, password, &db).await;
                        let (success, refresh_token, access_token) = match login_attempt {
                            Ok(result) => { 
                                let refresh_token = roll_refresh_token(result, &db).await;
                                let access_token = roll_access_token(refresh_token, &db).await;
                                (true, Some(refresh_token), access_token) 
                            },
                            Err(_) => { (false, None, None) }
                        };
                        // TODO: handle the error of time going before UNIX_EPOCH, set time to 0?
                        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis() as u64;
                        let confirm_message = ServerMessage {
                            id: id,
                            timestamp: current_time,
                            payload: ServerPayload::ConfirmLogin{success, refresh_token, access_token},
                        };
                        let message_bytes = rmp_serde::to_vec(&confirm_message).unwrap();
                        let _ = write.send(tokio_tungstenite::tungstenite::Message::binary(message_bytes)).await;

                    },
                    ClientPayload::UpdateCheckbox {..} => {

                    },
                    ClientPayload::UpdateCheckboxDetails {..} => {

                    },
                    ClientPayload::RequestSync(_time) => {
                        // TODO: Sync

                        // Testing ending image over websocket

                        let image_file = fs::read("maps/t01.png");
                        // TODO: handle Err Result
                        if let Ok(image_file) = image_file {
                            let current_time = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis() as u64;
                            let image_message = ServerMessage {
                                id,
                                timestamp: current_time,
                                payload: ServerPayload::MapImage(image_file),
                            };
                            let message_bytes = rmp_serde::to_vec(&image_message).unwrap();
                            let _ = write.send(tokio_tungstenite::tungstenite::Message::binary(message_bytes)).await;
                        }
                    },
                    ClientPayload::SetLowDataMode(..) => {

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