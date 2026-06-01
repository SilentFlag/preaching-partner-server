use core::panic;
use preaching_partner_server::database;
use preaching_partner_server::handle_connection;
use tokio::net::TcpListener;

// TODO: Work out how to import this so I can pass the correct type to the handle connection function
// mod auth;
// use auth::database;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:9001")
        .await
        .expect("Failed to bind");

    println!("Server listening on ws://0.0.0.0:9001");

    // Open Database
    let data_storage = match database::MyDatabase::new().await {
        Ok(database) => database,
        Err(error) => panic!("{}", error),
    };

    while let Ok((stream, _)) = listener.accept().await {
        let pool = data_storage.clone();
        tokio::spawn(handle_connection(stream, pool));
    }
}
