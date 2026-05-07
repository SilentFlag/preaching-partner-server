use crate::datatypes::{ServerMessage, ServerPayload};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use sqlx::Row;
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
pub async fn sync_user(db: &sqlx::Pool<sqlx::Sqlite>, write: &mut WsSink, last_sync: u64, id: u32) {
    // Select all the images from the database that have been updated since the last sync time

    let sync_vector = rmp_serde::to_vec(&last_sync);
    match sync_vector {
        Ok(sync_vec) => {
            // TODO: Bind last sync time
            let get_user_id_query =
                sqlx::query("SELECT * FROM maps WHERE updated >= ?").bind(hex::encode(sync_vec));

            let rows_result = get_user_id_query.fetch_all(db).await;

            if let Ok(rows) = rows_result {
                if rows.len() > 0 {
                    // TODO: Loop through the rows and send the images.
                    // user_id = rows[0].get("user");

                    for row in rows {
                        let try_file_name: Result<String, sqlx::Error> = row.try_get("file_name");

                        match try_file_name {
                            Ok(file_name) => {
                                let image_file = fs::read(format!("maps/{}", file_name));
                                // TODO: handle Err Result
                                if let Ok(image_file) = image_file {
                                    let current_time = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .expect("Time went backwards")
                                        .as_millis()
                                        as u64;
                                    let image_message = ServerMessage {
                                        id,
                                        timestamp: current_time,
                                        payload: ServerPayload::MapImage(
                                            String::from(file_name),
                                            image_file,
                                        ),
                                    };
                                    let message_bytes = rmp_serde::to_vec(&image_message).unwrap();
                                    let _ = write
                                        .send(tokio_tungstenite::tungstenite::Message::binary(
                                            message_bytes,
                                        ))
                                        .await;
                                }
                            }
                            Err(_) => {
                                // TODO: Handle this error
                            }
                        }
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
            }
        }
        Err(_) => {
            //TODO: Something
            println!("Error happened 1837");
        }
    }
}
