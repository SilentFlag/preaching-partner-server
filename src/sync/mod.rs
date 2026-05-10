use crate::datatypes::{CongDetails, MapDetails, ServerMessage, ServerPayload};
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
            let congregations = get_congregations(id, db).await;
            match congregations {
                Ok(congregations) => {
                    let _sync_congregations_result =
                        sync_congregations(id, last_sync, congregations, write, db).await;
                    let _sync_maps_result = sync_maps(sync_vec, id, write, db).await;
                }
                Err(_) => {
                    // TODO: Handle this error
                    println!("An error occured");
                }
            }
        }
        Err(_) => {
            //TODO: Something
            println!("Error happened 1837");
        }
    }
}

/// Sync the clients database of congregations as they are in with the database on the server
///
/// Parameters:
///     user_id: Id of the user as found in the users table
///     last_sync: Timestamp of the last time the client has synced their congregations
///     congregations: Vector of all congregations linked to that person as found in the user_cong_pair table
///     write: Mutable WsSink reference for the function to send messsages to the client
///     db: Reference to the database to be able to delete any records as necessary
///
/// Return Value:
///     Ok(()) is returned when the function is successful.
///     Err(sqlx::Error) is returned when there is a problem with the database
///
/// TODO: Handle error of a failure to delete a record from the database, Potentially leave it and have the get_congregations function handle it, when there is a congregation with an old updated timestamp but with deleted as true.
/// TODO: Update congregation vector and return that also
async fn sync_congregations(
    user_id: u32,
    last_sync: u64,
    congregations: Vec<CongDetails>,
    write: &mut WsSink,
    db: &sqlx::Pool<sqlx::Sqlite>,
) -> Result<(), sqlx::Error> {
    for cong in congregations {
        if cong.updated > last_sync {
            let payload = ServerPayload::SyncCong {
                cong_id: cong.cong_id,
                remove: cong.remove,
            };
            let message = ServerMessage {
                id: 0,
                timestamp: cong.timestamp,
                payload,
            };
            // Send message
            let message_bytes = rmp_serde::to_vec(&message).unwrap();
            let _ = write
                .send(tokio_tungstenite::tungstenite::Message::binary(
                    message_bytes,
                ))
                .await;

            if cong.remove == true {
                let update_query = sqlx::query(
                    "DELETE FROM user_cong_pair WHERE user_id = ? AND congregation_id = ?",
                )
                .bind(&user_id)
                .bind(cong.cong_id);

                let rows_result = update_query.execute(db).await;
                if let Err(error) = rows_result {
                    // TODO: handle this error
                    println!("Something went wrong");
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}

/// Sync the clients database of maps and images of maps to match the server
///
/// Parameter:
///     sync_vec:
///     id:
///     write: Mutable WsSink reference for the function to send messsages to the client
///     db: Reference to the database to be able to delete any records as necessary
///
/// Return Value:
///     Ok(()) is returned when the function is successful.
///     Err(sqlx::Error) is returned when there is a problem with the database
///
/// TODO: Update to get maps only for the persons congregation, add congregation vector
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
                                id: 0,
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

/// Given a SqliteRow, return the details of the map
///
/// Parameter:
///     row: A SqliteRow of the maps table
///
/// Return Value:
///     Ok(MapDetails): Map details from row returned when successful
///     Err(sqlx::Error): Error when getting the collumns, caused by row from the wrong table
///
/// TODO: Put in the column names
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

/// Get all congregations relevent to a particular user
///
/// Parameters:
///     user_id: Id of the user as found in the users table
///     db: Reference to the database
///
/// Return Value:
///     Ok(Vec<CongDetails>): Returned when getting congregations is successful, vector of all relevent congregations
///     Err(sqlx::Error): Returned when there is a problem with the database
///
async fn get_congregations(
    user_id: u32,
    db: &sqlx::Pool<sqlx::Sqlite>,
) -> Result<Vec<CongDetails>, sqlx::Error> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;

    let query = sqlx::query(
        "SELECT congregation_id, deleted, updated FROM user_cong_pair WHERE user_id = ?",
    )
    .bind(&user_id);

    let rows_result = query.fetch_all(db).await;

    let mut congregations: Vec<CongDetails> = vec![];

    match rows_result {
        Ok(rows) => {
            for row in rows {
                let cong_id: u32 = row.try_get("congregation_id")?;
                let remove: bool = row.try_get("deleted")?;
                let updated: u64 = 0;
                congregations.push(CongDetails {
                    cong_id,
                    timestamp,
                    remove,
                    updated,
                });
            }
        }
        Err(error) => {
            return Err(error);
        }
    }

    Ok(congregations)
}
