use axum::{
    Router,
    body::Bytes,
    extract::{State, ws::WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use preaching_partner_server::auth;
use preaching_partner_server::database::MyDatabase;
use preaching_partner_server::datatypes;
use preaching_partner_server::handle_connection;
mod services;

// Here I have a code snippit which needs to call the function handle_connection and pass the WebSocket and db to it. How should it be written

async fn ws_handler(ws: WebSocketUpgrade, State(db): State<MyDatabase>) -> Response {
    ws.on_upgrade(move |socket| handle_connection(socket, db))
}

async fn login_handler(State(db): State<MyDatabase>, body: Bytes) -> impl IntoResponse {
    let body = body.to_vec();
    let decoded: datatypes::ClientMessage = match rmp_serde::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let response_bytes = match decoded.payload {
        datatypes::ClientPayload::Login { name, password } => {
            services::login_attempt(name, password, db.clone())
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        }
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };

    (
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        response_bytes,
    )
        .into_response()
}

#[tokio::main]
async fn main() {
    let data_storage: MyDatabase = match MyDatabase::new().await {
        Ok(database) => database,
        Err(error) => panic!("{}", error),
    };

    let app = Router::new()
        .route("/", get(|| async { "Hello" }))
        .route("/login", post(login_handler))
        .route("/ws", get(ws_handler))
        .with_state(data_storage);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:9001")
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
}
