use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};


pub mod datatypes;
use crate::datatypes::{ClientMessage, ClientPayload, ServerPayload, ServerMessage};

mod account;
use account::{login, roll_refresh_token, roll_access_token};


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
                        let login_attempt = login(name, password, &db).await; // TODO: login returns userid in Ok()
                        let (success, refresh_token, access_token) = match login_attempt {
                            Ok(result) => { 
                                let refresh_token = roll_refresh_token(result, &db).await;
                                let access_token = roll_access_token(refresh_token, &db).await;
                                (true, Some(refresh_token), access_token) 
                            },
                            Err(_) => { (false, None, None) }
                        };
                        
                        let confirm_message = ServerMessage {
                            id: id,
                            payload: ServerPayload::ConfirmLogin{success, refresh_token, access_token},
                        };
                        let message_bytes = rmp_serde::to_vec(&confirm_message).unwrap();
                        let _ = write.send(tokio_tungstenite::tungstenite::Message::binary(message_bytes)).await;

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