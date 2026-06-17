// use core::panic;
use preaching_partner_server::database::{self, MyDatabase};
// use preaching_partner_server::handle_connection;
// use tokio::net::TcpListener;

// TODO: Work out how to import this so I can pass the correct type to the handle connection function
// mod auth;
// use auth::database;

// #[tokio::main]
// async fn main() {
//     let listener = TcpListener::bind("0.0.0.0:9001")
//         .await
//         .expect("Failed to bind");

//     println!("Server listening on ws://0.0.0.0:9001");

//     // Open Database
//     let data_storage = match database::MyDatabase::new().await {
//         Ok(database) => database,
//         Err(error) => panic!("{}", error),
//     };

//     while let Ok((stream, _)) = listener.accept().await {
//         let pool = data_storage.clone();
//         tokio::spawn(handle_connection(stream, pool));
//     }
// }

use axum::{
    Router,
    extract::{State, ws::WebSocketUpgrade},
    response::Response,
    routing::get,
};
use preaching_partner_server::handle_connection;

// Here I have a code snippit which needs to call the function handle_connection and pass the WebSocket and db to it. How should it be written

async fn ws_handler(ws: WebSocketUpgrade, State(db): State<MyDatabase>) -> Response {
    ws.on_upgrade(move |socket| handle_connection(socket, db))
}

#[tokio::main]
async fn main() {
    let data_storage = match database::MyDatabase::new().await {
        Ok(database) => database,
        Err(error) => panic!("{}", error),
    };

    let app = Router::new()
        .route("/", get(|| async { "Hello" }))
        .route("/login", get(|| async { "Login Page" }))
        .route("/ws", get(ws_handler))
        .with_state(data_storage);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:9001")
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
}
