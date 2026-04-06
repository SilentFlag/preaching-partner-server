use std::{str::FromStr};

use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

use futures_util::{SinkExt, StreamExt};

use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};

mod datatypes;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:9001")
        .await
        .expect("Failed to bind");

    println!("Server listening on ws://0.0.0.0:9001");

    // Open Database
    let my_pool_option = SqliteConnectOptions::from_str("sqlite://database/data.db");
    let conn = match my_pool_option {
        Ok(my_pool_option) => {
            let my_pool_option = my_pool_option.journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
            let conn = SqlitePool::connect_with(my_pool_option).await;
            match conn {
                Ok(conn) => {
                    conn
                }
                Err(error) => {
                    panic!("Connection to database failed: {:?}", error);
                }
            }
        }
        Err(error) => {
            panic!("Database Options Failed: {:?}", error);
        }
    };
    
    while let Ok((stream, _)) = listener.accept().await {
        let pool = conn.clone();
        tokio::spawn(handle_connection(stream, pool));
    }
}

async fn handle_connection(stream: tokio::net::TcpStream, db: sqlx::Pool<sqlx::Sqlite>) {
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
                println!("Received binary message: {:?}", bin);
                // Handle binary messages if needed
                let decoded: datatypes::ClientMessage = rmp_serde::from_slice(&bin).unwrap();

                match decoded {
                    datatypes::ClientMessage::Login {name, password} => {
                        let query = format!("SELECT * FROM users WHERE firstname = \"{}\" AND password = \"{}\"", name, password);
                        let rows = sqlx::query(&query).execute(&db).await;
                        println!("{:?} with query {:?}", rows, query);
                    },
                    datatypes::ClientMessage::UpdateCheckbox {..} => {

                    },
                    datatypes::ClientMessage::UpdateCheckboxDetails {..} => {

                    },
                    datatypes::ClientMessage::SetLowDataMode(..) => {

                    }
                }

                let confirm_message = datatypes::ServerMessage::Confirm(true);
                let message_bytes = rmp_serde::to_vec(&confirm_message).unwrap();
                let _ = write.send(tokio_tungstenite::tungstenite::Message::binary(message_bytes)).await;
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