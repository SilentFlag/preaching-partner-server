use crate::datatypes::{
    CategoryDetails, CongDetails, GroupDetails, MapDetails, ServerMessage, ServerPayload,
    UserPublicDetails,
};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, vec};
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

type WsStream = WebSocketStream<TcpStream>;
type WsSink = SplitSink<WsStream, Message>;

/// Send the user updated data for all changes since the user last opened the app
///
/// TODO: Fully sync user
pub async fn sync_user(db: &sqlx::Pool<sqlx::Sqlite>, write: &mut WsSink, last_sync: u64, id: u32) {
    // Select all the images from the database that have been updated since the last sync time

    let sync_vector = rmp_serde::to_vec(&last_sync);
    match sync_vector {
        Ok(sync_vec) => {
            // TODO: Sync map dependencies first (categories, users, congregation)
            let congregations = get_congregations(id, db).await;
            match congregations {
                Ok(congregations) => {
                    // TODO: Handle errors returned by sync functions
                    let _sync_congregations_result =
                        sync_congregations(id, last_sync, &congregations, write, db).await;

                    let _sync_categories_result =
                        sync_categories(sync_vec, last_sync, &congregations, db).await;

                    let _sync_groups_result = sync_service_groups(id, last_sync, write, db).await;

                    // let _sync_users_result = sync_users().await;

                    // TODO: Uncomment this when all dependent tables have been implemented
                    // let _sync_maps_result = sync_maps(sync_vec, id, write, db).await;
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
    congregations: &Vec<CongDetails>,
    write: &mut WsSink,
    db: &sqlx::Pool<sqlx::Sqlite>,
) -> Result<(), sqlx::Error> {
    for cong in congregations {
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
        "SELECT user_cong_pair.congregation_id, congregation.name, user_cong_pair.deleted, user_cong_pair.updated FROM user_cong_pair WHERE user_id = ? INNER JOIN congregation ON user_cong_pair.congregation_id=congregation.id",
    )
    .bind(&user_id);

    let rows_result = query.fetch_all(db).await;

    let mut congregations: Vec<CongDetails> = vec![];

    match rows_result {
        Ok(rows) => {
            for row in rows {
                let cong_id: u32 = row.try_get("congregation_id")?;
                let cong_name: String = row.try_get("name")?;
                let remove: bool = row.try_get("deleted")?;
                let updated: u64 = 0;
                congregations.push(CongDetails {
                    cong_id,
                    cong_name,
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
    last_sync_vec: Vec<u8>,
    last_sync: u64,
    congregations: &Vec<CongDetails>,
    db: &sqlx::Pool<sqlx::Sqlite>,
) -> Result<(), sqlx::Error> {
    let get_maps_query =
        sqlx::query("SELECT * FROM categories WHERE updated >= ?").bind(hex::encode(last_sync_vec));

    let rows_result = get_maps_query.fetch_all(db).await;

    match rows_result {
        Ok(rows) => {
            let mut cong_ids = HashSet::new();
            for cong in congregations {
                cong_ids.insert(cong.cong_id);
            }
            for row in rows {
                let category_details = get_category_details(row).await;
                match category_details {
                    Ok(category_details) => {
                        if category_details.updated > last_sync
                            && cong_ids.contains(&category_details.id)
                        {
                            // TODO: send message to client to update
                        }
                    }
                    Err(error) => {
                        return Err(error);
                    }
                }
            }
        }
        Err(error) => {
            return Err(error);
        }
    }

    Ok(())
}

/// Given a SqliteRow, return the details of the category
///
/// Parameter:
///     row: A SqliteRow of the categories table
///
/// Return Value:
///     Ok(MapDetails): Category details from row returned when successful
///     Err(sqlx::Error): Error when getting the collumns, caused by row from the wrong table
///
async fn get_category_details(row: SqliteRow) -> Result<CategoryDetails, sqlx::Error> {
    let id = row.try_get("id")?;
    let name = row.try_get("name")?;
    let prefix = row.try_get("prefix")?;
    let congregation = row.try_get("congregation")?;
    let updated = row.try_get("updated")?;
    Ok(CategoryDetails {
        id,
        name,
        prefix,
        congregation,
        updated,
    })
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
    db: &sqlx::Pool<sqlx::Sqlite>,
) -> Result<(), sqlx::Error> {
    let groups = get_groups(user_id, db).await;
    match groups {
        Ok(groups) => {
            for group in groups {
                if group.updated > last_sync {
                    if group.pair_deleted {
                        // Delete record of user_group_pair
                        let update_query = sqlx::query(
                            "DELETE FROM user_group_pair WHERE user_id = ? AND group_id = ?",
                        )
                        .bind(&user_id)
                        .bind(group.id);

                        let rows_result = update_query.execute(db).await;
                        if let Err(error) = rows_result {
                            // TODO: handle this error
                            println!("Something went wrong");
                            return Err(error);
                        }

                        // Check if record of group needs to be deleted
                        if group.group_deleted {
                            let query = sqlx::query(
                                "SELECT user_id FROM user_group_pair WHERE group_id = ?",
                            )
                            .bind(group.id);

                            let rows_result = query.fetch_all(db).await;

                            match rows_result {
                                Ok(rows) => {
                                    if rows.len() == 0 {
                                        let update_query =
                                            sqlx::query("DELETE FROM service_group WHERE id = ?")
                                                .bind(group.id);

                                        let rows_result = update_query.execute(db).await;
                                        if let Err(error) = rows_result {
                                            // TODO: handle this error
                                            println!("Something went wrong");
                                            return Err(error);
                                        }
                                    }
                                }
                                Err(_error) => {
                                    // TODO: handle this error
                                }
                            }
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

// TODO: write docs
async fn get_groups(
    user_id: u32,
    db: &sqlx::Pool<sqlx::Sqlite>,
) -> Result<Vec<GroupDetails>, sqlx::Error> {
    let query = sqlx::query("SELECT user_group_pair.group_id AS group_id, user_group_pair.deleted AS pair_deleted, user_group_pair.updated AS pair_updated, service_group.name AS name, service_group.elder AS elder, service_group.deleted AS group_deleted, service_group.updated AS group_updated, service_group.congregation AS congregation  FROM user_group_pair INNER JOIN service_group ON service_group.id=user_group_pair.group_id WHERE user_id = ?")
        .bind(&user_id);

    let rows_result = query.fetch_all(db).await;

    let mut groups: Vec<GroupDetails> = vec![];

    match rows_result {
        Ok(rows) => {
            for row in rows {
                let group_details = get_group_details(row);
                match group_details {
                    Ok(details) => {
                        groups.push(details);
                    }
                    Err(_error) => {
                        // TODO: handle error, don't return in case some groups are found?
                    }
                }
            }
        }
        Err(error) => {
            return Err(error);
        }
    }

    Ok(groups)
}

/// Given a SqliteRow of a groups details, return the formatted details
///
/// Query for rows: "SELECT user_group_pair.group_id AS group_id, user_group_pair.deleted AS pair_deleted, user_group_pair.updated AS pair_updated, service_group.name AS name, service_group.elder AS elder, service_group.deleted AS group_deleted, service_group.updated AS group_updated, service_group.congregation AS congregation  FROM user_group_pair INNER JOIN service_group ON service_group.id=user_group_pair.group_id WHERE user_id = ?"
///
/// Parameters:
///     row: SqliteRow of the group details
///
/// Return Value:
///     Ok(GroupDetails): Function successful
///     Err(sqlx::Error): Sqlx Error occured
fn get_group_details(row: SqliteRow) -> Result<GroupDetails, sqlx::Error> {
    let id = row.try_get("group_id")?;
    let name: String = row.try_get("name")?;
    let cong: u32 = row.try_get("congregation")?;
    let elder: u32 = row.try_get("elder")?;
    let group_updated: u64 = row.try_get("group_updated")?;
    let pair_updated: u64 = row.try_get("pair_updated")?;
    let updated: u64 = if group_updated > pair_updated {
        group_updated
    } else {
        pair_updated
    };
    let group_deleted: bool = row.try_get("delted")?;
    let pair_deleted: bool = row.try_get("pair_delted")?;
    Ok(GroupDetails {
        id,
        name,
        cong,
        elder,
        updated,
        group_deleted,
        pair_deleted,
    })
}

/// TODO: Write this function
async fn _sync_users(db: &sqlx::Pool<sqlx::Sqlite>) -> Result<(), sqlx::Error> {
    let users = get_users(db).await;
    Ok(())
}

/// TODO: Write docs
async fn get_users(db: &sqlx::Pool<sqlx::Sqlite>) -> Result<Vec<UserPublicDetails>, sqlx::Error> {
    let query = sqlx::query("SELECT id, firstname, lastname, updated FROM users");

    let rows_result = query.fetch_all(db).await;

    let mut users: Vec<UserPublicDetails> = vec![];

    match rows_result {
        Ok(rows) => {
            for row in rows {
                let user_details = get_user_details(row);
                match user_details {
                    Ok(user_details) => {
                        users.push(user_details);
                    }
                    Err(error) => {
                        // TODO: handle this error
                    }
                }
            }
        }
        Err(error) => {
            return Err(error);
        }
    }

    Ok(users)
}

/// TODO: Write docs
fn get_user_details(row: SqliteRow) -> Result<UserPublicDetails, sqlx::Error> {
    let id = row.try_get("id")?;
    let firstname: String = row.try_get("firstname")?;
    let lastname: String = row.try_get("lastname")?;
    let updated: u64 = row.try_get("updated")?;
    let name = format!("{} {}", firstname, lastname);
    Ok(UserPublicDetails { id, name, updated })
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
async fn _sync_maps(
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
                let details = get_map_details(row);

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
fn get_map_details(row: SqliteRow) -> Result<MapDetails, sqlx::Error> {
    let image_name: String = row.try_get("file_name")?;
    let assignee: u32 = row.try_get("assignee")?;
    let assigner: u32 = row.try_get("assigner")?;
    let category: u32 = row.try_get("category")?;
    Ok(MapDetails {
        image_name,
        assignee,
        assigner,
        category,
    })
}
