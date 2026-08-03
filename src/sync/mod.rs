use crate::database::MyDatabase;
use crate::datatypes::{
    AddressDetails, CategoryDetails, CongDetails, DbError, GroupDetails, MapDetails, ServerMessage,
    ServerPayload, StreetDetails, SyncInformation, UserPublicDetails,
};
use axum::extract::ws::{Message, WebSocket};
// use core::sync;
// use futures_util::{SinkExt, stream::SplitSink};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, vec};
// use tokio::net::TcpStream;
// use tokio_tungstenite::WebSocketStream;
// use tokio_tungstenite::tungstenite::Message;

// type WsStream = WebSocketStream<TcpStream>;
// type WsSink = SplitSink<WsStream, Message>;

/// Send the user updated data for all changes since the user last opened the app
///
/// TODO: Update the error return to also include edge case errors
/// TODO: Authorise updates
pub async fn sync_user(
    db: MyDatabase,
    socket: &mut WebSocket,
    last_sync: u32,
    id: u32,
) -> Result<(), DbError> {
    // Select all the images from the database that have been updated since the last sync time

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as u32;

    let congregations = sync_congregations(id, last_sync, db.clone()).await?;

    let categories = sync_categories(last_sync, &congregations, db.clone()).await?;

    let service_groups = sync_service_groups(id, last_sync, db.clone()).await?;

    let users = sync_users(last_sync, id, db.clone()).await?;

    // TODO Get maps, streets, and addresses
    let maps = sync_maps(last_sync, id, db.clone()).await?;

    let streets = sync_streets(last_sync, &maps, db.clone()).await?;

    // TODO: Not sync addresses?
    let addresses = sync_addresses(last_sync, &streets, db.clone()).await?;

    println!("Preparing message");
    let sync_details = SyncInformation {
        congregations,
        categories,
        service_groups,
        users,
        maps,
        streets,
        addresses,
    };

    let message = ServerMessage {
        id,
        timestamp,
        payload: ServerPayload::SyncInformation(sync_details),
    };
    let message_bytes = rmp_serde::to_vec(&message).expect("failed to encode sync message");
    let message_to_send = Message::binary(message_bytes);
    let _send_result = socket.send(message_to_send).await; // TODO: handle error
    println!("sent message");
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
/// TODO: Add compatibility with ability to delete congregations
/// TODO: Update congregation vector to remove deleted congs and return that also
/// TODO: use last_sync
async fn sync_congregations(
    user_id: u32,
    _last_sync: u32,
    db: MyDatabase,
) -> Result<Vec<CongDetails>, DbError> {
    let congregations_result = db.get_congregations(user_id).await;

    match congregations_result {
        Ok(congregations) => Ok(congregations),
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
/// TODO: Add compatibility with ability to delete categories
/// TODO: Use congregations
async fn sync_categories(
    last_sync_vec: u32,
    _congregations: &Vec<CongDetails>,
    db: MyDatabase,
) -> Result<Vec<CategoryDetails>, DbError> {
    // TODO: update get_categories function
    let categories = db.get_categories(last_sync_vec).await?;
    Ok(categories)
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
/// TODO: get_groups() function accept last_sync and use that in sql query rather than filter after
async fn sync_service_groups(
    user_id: u32,
    last_sync: u32,
    db: MyDatabase,
) -> Result<Vec<GroupDetails>, DbError> {
    let groups = db.get_groups(user_id).await?;

    for group in &groups {
        if group.updated > last_sync {
            if group.pair_deleted {
                // Delete record of user_group_pair
                let _ = db.delete_user_group_record(user_id, group.id).await?;

                // Check if record of group needs to be deleted
                // TODO: Check if this needs to check the existance of other group pairs
                if group.group_deleted {
                    let _ = db.delete_group_record(group.id).await?;
                }
            }
        }
    }

    Ok(groups)
}

///
/// Should the sync users only sync requested users to protect privacy? It won't protect much as all people of a congregation will know each other already
async fn sync_users(
    last_sync_vec: u32,
    user_id: u32,
    db: MyDatabase,
) -> Result<Vec<UserPublicDetails>, DbError> {
    let users = db.get_users(last_sync_vec, user_id).await?;
    Ok(users)
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
async fn sync_maps(last_sync: u32, id: u32, db: MyDatabase) -> Result<Vec<MapDetails>, DbError> {
    let maps = db.get_maps(id, last_sync).await?;

    let mut complete_maps = vec![];
    // Send Data
    for map_details in maps {
        let image_file = fs::read(format!("maps/{}", map_details.image_name));
        // TODO: handle Err Result
        complete_maps.push(MapDetails {
            id: map_details.id,
            name: map_details.name,
            image_name: map_details.image_name,
            assignee: map_details.assignee,
            assigner: map_details.assigner,
            image: if let Ok(image) = image_file {
                Some(image)
            } else {
                None
            },
            category: map_details.category,
            deleted: map_details.deleted,
        });
    }
    return Ok(complete_maps);
}

// TODO: Be more selective in streets, give map id?
async fn sync_streets(
    _lasy_sync: u32,
    _maps: &Vec<MapDetails>,
    db: MyDatabase,
) -> Result<Vec<StreetDetails>, DbError> {
    let streets = db.get_streets().await?;
    Ok(streets)
}

// TODO: be more selective, map id or street id?
async fn sync_addresses(
    _last_sync: u32,
    _streets: &Vec<StreetDetails>,
    db: MyDatabase,
) -> Result<Vec<AddressDetails>, DbError> {
    let addresses = db.get_addresses().await?;
    Ok(addresses)
}
