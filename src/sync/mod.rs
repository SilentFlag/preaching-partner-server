use crate::datatypes::{ServerMessage, ServerPayload};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

type WsStream = WebSocketStream<TcpStream>;
type WsSink = SplitSink<WsStream, Message>;

/// Send the user updated data for all changes since the user last opened the app
///
/// TODO: Fully sync user
/// Currently only sends an image
pub async fn sync_user(write: &mut WsSink, _last_sync: u64, id: u32) {
    // Testing ending image over websocket
    // Testing ending image over websocket
    let image_name = "t01";
    let image_file = fs::read(format!("maps/{}.png", image_name));
    // TODO: handle Err Result
    if let Ok(image_file) = image_file {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let image_message = ServerMessage {
            id,
            timestamp: current_time,
            payload: ServerPayload::MapImage(String::from(image_name), image_file),
        };
        let message_bytes = rmp_serde::to_vec(&image_message).unwrap();
        let _ = write
            .send(tokio_tungstenite::tungstenite::Message::binary(
                message_bytes,
            ))
            .await;
    }

    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;
    let sync_complete_message = ServerMessage {
        id,
        timestamp: current_time,
        payload: ServerPayload::SyncComplete,
    };
    let message_bytes = rmp_serde::to_vec(&sync_complete_message).unwrap();
    let _ = write
        .send(tokio_tungstenite::tungstenite::Message::binary(
            message_bytes,
        ))
        .await;
}
