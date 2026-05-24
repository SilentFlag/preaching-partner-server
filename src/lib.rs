use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{accept_async, tungstenite::Message};

mod authorise;
mod datatypes;
use crate::authorise::check_permissions;
use crate::datatypes::ClientMessage;

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

                // TODO: Confirm the token sent with the message
                let decoded: ClientMessage = rmp_serde::from_slice(&bin).unwrap();
                let token = decoded.token;
                let actions = decoded.action_list;

                let user_authorised = check_permissions(token, actions);

                match user_authorised {
                    Ok(authorised) => if authorised {},
                    Err(error) => {
                        // TODO: handle error
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
