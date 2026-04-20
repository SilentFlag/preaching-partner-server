use std::{str::FromStr};
use tokio::net::TcpListener;
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use preaching_partner_server::handle_connection;

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