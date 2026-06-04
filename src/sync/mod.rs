use crate::database::MyDatabase;
use crate::datatypes::{CongDetails, DbError, ServerMessage, ServerPayload};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use std::collections::HashSet;
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
/// TODO: Update to use the MyDatabase abstraction as a parameter
/// TODO: Update the error return to also include edge case errors
/// TODO: Authorise updates
pub async fn sync_user(
    db: MyDatabase,
    write: &mut WsSink,
    last_sync: u64,
    id: u32,
) -> Result<(), DbError> {
    // Select all the images from the database that have been updated since the last sync time

    let sync_vector = rmp_serde::to_vec(&last_sync);
    match sync_vector {
        Ok(sync_vec) => {
            let congregations = sync_congregations(id, last_sync, write, db.clone()).await?;

            // TODO: Send messages to client fo categories
            let _ = sync_categories(&sync_vec, &congregations, db.clone()).await?;

            let _ = sync_service_groups(id, last_sync, write, db.clone()).await?;

            let _ = sync_users(&sync_vec, id, db.clone()).await;

            // TODO: Uncomment this when all dependent tables have been implemented
            let _sync_maps_result = sync_maps(&sync_vec, id, write, db.clone()).await;
        }
        Err(_) => {
            //TODO: Something
            println!("Error happened 1837");
        }
    }
    Ok(())
}

/// Sync the client's database of congregations as they are in the database on the server
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
///
/// TODO: Handle error of a failure to delete a record from the database, Potentially leave it and have the get_congregations function handle it, when there is a congregation with an old updated timestamp but with deleted as true.
/// TODO: Update congregation vector to remove deleted congs and return that also
async fn sync_congregations(
    user_id: u32,
    last_sync: u64,
    write: &mut WsSink,
    db: MyDatabase,
) -> Result<Vec<CongDetails>, DbError> {
    let congregations_result = db.get_congregations(user_id).await;

    match congregations_result {
        Ok(congregations) => {
            for cong in congregations.iter().clone() {
                if cong.updated > last_sync {
                    let payload = ServerPayload::SyncCong {
                        cong_id: cong.cong_id,
                        cong_name: cong.cong_name.clone(),
                        remove: cong.remove,
                    };
                    let message = ServerMessage {
                        id: 0,
                        timestamp: cong.timestamp,
                        payload,
                    };
                    // Send message
                    // TODO: Handle error of message failing to send
                    let message_bytes = rmp_serde::to_vec(&message).unwrap();
                    let _ = write
                        .send(tokio_tungstenite::tungstenite::Message::binary(
                            message_bytes,
                        ))
                        .await;

                    if cong.remove == true {
                        let _ = db.delete_user_cong_record(user_id, cong.cong_id);
                    }
                }
            }
            Ok(congregations)
        }
        Err(error) => {
            return Err(error);
        }
    }
}

/// Sync the client's database of categories as they are in the database on the server
///
/// Parameters:
///     last_sync_vec: Vector version of the last time the client has synced the categories
///     last_sync: u64 version of the last time the client has synced the categories
///     congregations: Vector of the congregations the client is in
///     db: Reference to the database to be able to delete any records as necessary
///
/// Return Values:
///     Ok(()) is returned when the function is successful.
///     Err(sqlx::Error) is returned when there is a problem with the database
///
/// TODO: Send the message to the client to sync the category
async fn sync_categories(
    last_sync_vec: &Vec<u8>,
    congregations: &Vec<CongDetails>,
    db: MyDatabase,
) -> Result<(), DbError> {
    let categories = db.get_categories(last_sync_vec).await?;

    let mut cong_ids = HashSet::new();
    for cong in congregations {
        cong_ids.insert(cong.cong_id);
    }

    for row in categories {
        if cong_ids.contains(&row.congregation) {
            // TODO: send message to client to update
        }
    }

    Ok(())
}

/// Given a user_id and last sync time, sync the user with the database on the server
///
/// Parameters:
///     user_id: Id of the user to update
///     last_sync: Timestamp of the latest sync
///     write: Mutable WsSink reference for the function to send messsages to the client
///     db: Reference to the database to be able to delete any records as necessary
///
/// Return Value:
///     Ok(()): Sync Successful
///     Err(sqlx::Error): Something went wrong
///
async fn sync_service_groups(
    user_id: u32,
    last_sync: u64,
    write: &mut WsSink,
    db: MyDatabase,
) -> Result<(), DbError> {
    let groups = db.get_groups(user_id).await;
    match groups {
        Ok(groups) => {
            for group in groups {
                if group.updated > last_sync {
                    if group.pair_deleted {
                        // Delete record of user_group_pair
                        let _ = db.delete_user_group_record(user_id, group.id).await?;

                        // Check if record of group needs to be deleted
                        if group.group_deleted {
                            let _ = db.delete_group_record(group.id).await?;
                        }

                        // Send message to delete
                        let message = ServerPayload::SyncGroup {
                            id: group.id,
                            name: group.name,
                            cong: group.cong,
                            elder: group.elder,
                            updated: group.updated,
                            deleted: true,
                        };
                        let message_bytes = rmp_serde::to_vec(&message).unwrap();
                        let _ = write
                            .send(tokio_tungstenite::tungstenite::Message::binary(
                                message_bytes,
                            ))
                            .await;
                    } else {
                        // Send message to update
                        let message = ServerPayload::SyncGroup {
                            id: group.id,
                            name: group.name,
                            cong: group.cong,
                            elder: group.elder,
                            updated: group.updated,
                            deleted: false,
                        };
                        let message_bytes = rmp_serde::to_vec(&message).unwrap();
                        let _ = write
                            .send(tokio_tungstenite::tungstenite::Message::binary(
                                message_bytes,
                            ))
                            .await;
                    }
                }
            }
        }
        Err(_error) => {
            // TODO: handle error
        }
    }
    Ok(())
}

/// TODO: Write this function
/// Should the sync users only sync requested users to protect privacy? It won't protect much as all people of a congregation will know each other already
async fn sync_users(last_sync_vec: &Vec<u8>, user_id: u32, db: MyDatabase) -> Result<(), DbError> {
    let _users = db.get_users(last_sync_vec, user_id).await?;

    // TODO: Send users to client
    Ok(())
}

/// Sync the client's database of maps and images of maps to match the server
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
/// TODO: Handle error that get_map_details() returns
async fn sync_maps(
    sync_vec: &Vec<u8>,
    id: u32,
    write: &mut WsSink,
    db: MyDatabase,
) -> Result<(), DbError> {
    let maps = db.get_maps(id, sync_vec).await?;

    for map_details in maps {
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

    return Ok(());
}
