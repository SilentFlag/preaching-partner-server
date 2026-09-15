use axum::{
    Router,
    body::Bytes,
    extract::{State, ws::WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use preaching_partner_server::database::MyDatabase;
use preaching_partner_server::datatypes;
use preaching_partner_server::handle_connection;
use preaching_partner_server::{auth, datatypes::AppState};
use tokio::sync::broadcast;
mod services;
mod webpage;

async fn ws_handler(ws: WebSocketUpgrade, State(app_state): State<AppState>) -> Response {
    let db = app_state.db;
    let tx = app_state.tx;
    ws.on_upgrade(move |socket| handle_connection(socket, db, tx))
}

async fn login_handler(State(app_state): State<AppState>, body: Bytes) -> impl IntoResponse {
    let db = app_state.db;
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

    let (tx, _rx) = broadcast::channel(100);

    let app_state = datatypes::AppState {
        db: data_storage,
        tx,
    };

    let app = Router::new()
        .route("/", get(webpage::root))
        // Congregations
        .route("/congregation/new", get(webpage::add_congregation))
        .route("/congregation/new", post(services::add_congregation))
        .route(
            "/congregation/details/{id}",
            get(webpage::congregation_details),
        )
        // Groups
        .route("/groups/new/{id}", get(webpage::add_group))
        .route("/groups/new/{id}", post(services::add_group))
        // Users
        .route("/users/import/{id}", get(webpage::import_users))
        .route("/users/import/{id}", post(services::import_users))
        // TODO: Categories
        // add category
        // TODO: Maps
        // Add map
        // Edit map
        // add street
        // edit street
        // add addresses
        // edit addresses
        // App Connection
        .route("/login", post(login_handler))
        .route("/ws", get(ws_handler))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:9001")
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
}
