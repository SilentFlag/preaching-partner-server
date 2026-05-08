use crate::datatypes::{MapDetails, ServerMessage, ServerPayload};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
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
            // TODO: Sync map dependencies first (categories, users, congregation)

            let _sync_maps_result = sync_maps(sync_vec, id, write, db).await;
        }
        Err(_) => {
            //TODO: Something
            println!("Error happened 1837");
        }
    }
}

async fn sync_maps(
    sync_vec: Vec<u8>,
    id: u32,
    write: &mut WsSink,
    db: &sqlx::Pool<sqlx::Sqlite>,
) -> Result<(), sqlx::Error> {
    // TODO: Be more selective in the maps that are sent, only ones that are relevent to the user
    let get_maps_query =
        sqlx::query("SELECT * FROM maps WHERE updated >= ?").bind(hex::encode(sync_vec));

    let rows_result = get_maps_query.fetch_all(db).await;

    if let Ok(rows) = rows_result {
        if rows.len() > 0 {
            // TODO: Loop through the rows and send the images.
            // user_id = rows[0].get("user");

            for row in rows {
                let details = get_map_details(row).await;

                match details {
                    Ok(map_details) => {
                        let image_file = fs::read(format!("maps/{}", map_details.image_name));
                        // TODO: handle Err Result
                        if let Ok(image) = image_file {
                            let current_time = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_millis() as u64;
                            let image_message = ServerMessage {
                                id,
                                timestamp: current_time,
                                payload: ServerPayload::MapImage {
                                    image_name: map_details.image_name,
                                    image,
                                    assignee: map_details.assignee,
                                    assigner: map_details.assigner,
                                    category: map_details.category,
                                },
                            };
                            let message_bytes = rmp_serde::to_vec(&image_message).unwrap();
                            let _ = write
                                .send(tokio_tungstenite::tungstenite::Message::binary(
                                    message_bytes,
                                ))
                                .await;
                        }
                    }
                    Err(error) => {
                        // TODO: Handle error
                        println!("Error getting map details {:?}", error);
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
    return Ok(());
}

async fn get_map_details(row: SqliteRow) -> Result<MapDetails, sqlx::Error> {
    let image_name: String = row.try_get("file_name")?;
    let assignee: u32 = row.try_get("")?;
    let assigner: u32 = row.try_get("")?;
    let category: u32 = row.try_get("")?;
    Ok(MapDetails {
        image_name,
        assignee,
        assigner,
        category,
    })
}
