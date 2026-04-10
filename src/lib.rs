use tokio_tungstenite::{accept_async, tungstenite::Message};
// use sqlx::{Row, Column};
use futures_util::{SinkExt, StreamExt};


pub mod datatypes;
use crate::datatypes::{ClientMessage, ClientPayload, ServerPayload, ServerMessage};

mod account;
use account::login;

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
                        println!("{} {}", name, password);
                        let login_attempt = login(name, password, &db).await;

                        if let Ok(login_success) = login_attempt {
                            let confirm_message = ServerMessage {
                                id: id,
                                payload: ServerPayload::Confirm(login_success),
                            };
                            let message_bytes = rmp_serde::to_vec(&confirm_message).unwrap();
                            let _ = write.send(tokio_tungstenite::tungstenite::Message::binary(message_bytes)).await;
                        } else {
                            // TODO: log and inform user of error
                            let confirm_message = ServerMessage {
                                id: id,
                                payload: ServerPayload::Confirm(false),
                            };
                            let message_bytes = rmp_serde::to_vec(&confirm_message).unwrap();
                            let _ = write.send(tokio_tungstenite::tungstenite::Message::binary(message_bytes)).await;
                        }

                        // TODO: Create refresh and access tokens
                        

                    },
                    ClientPayload::UpdateCheckbox {..} => {

                    },
                    ClientPayload::UpdateCheckboxDetails {..} => {

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