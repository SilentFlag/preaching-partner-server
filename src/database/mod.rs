// use argon2::password_hash;
use crate::datatypes::{
    AddressDetails, AddressError, CategoryDetails, CongDetails, DbError, GroupDetails, MapDetails,
    StreetDetails, UserPublicDetails,
};
use blake2::{Blake2b512, Digest};
use sqlx::{Pool, Row, Sqlite, SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqliteRow};
use std::{str::FromStr, vec};

#[derive(Clone)]
pub struct MyDatabase {
    data: Pool<Sqlite>,
}

/// TODO: Better documentation
impl MyDatabase {
    /// Create new connection to the database
    pub async fn new() -> Result<Self, DbError> {
        let my_pool_option = SqliteConnectOptions::from_str("sqlite://database/data.db");
        let conn = match my_pool_option {
            Ok(my_pool_option) => {
                let my_pool_option =
                    my_pool_option.journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
                let conn = SqlitePool::connect_with(my_pool_option).await;
                match conn {
                    Ok(conn) => conn,
                    Err(error) => return Err(DbError::ConnectionFailure(error)),
                }
            }
            Err(error) => return Err(DbError::InvalidLocation(error)),
        };
        Ok(MyDatabase { data: conn })
    }

    // ------------------- CONGREGATION FUNCTIONS ----------

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
    /// TODO: Handle event where user cong pair not updated but cong is. Set updated timestamps on pairs when updating cong?
    pub async fn get_congregations(&self, user_id: u32) -> Result<Vec<CongDetails>, DbError> {
        let query = sqlx::query(
        "SELECT user_cong_pair.congregation_id, congregation.name, user_cong_pair.deleted, user_cong_pair.updated FROM user_cong_pair INNER JOIN congregation ON user_cong_pair.congregation_id=congregation.id WHERE user_id = ? ").bind(user_id);

        let rows_result = query.fetch_all(&self.data).await;

        let mut congregations: Vec<CongDetails> = vec![];

        match rows_result {
            Ok(rows) => {
                for row in rows {
                    let cong_details = cong_row_to_details(row);
                    match cong_details {
                        Ok(details) => {
                            congregations.push(details);
                        }
                        Err(error) => {
                            return Err(DbError::QueryFailure(error));
                        }
                    }
                }
            }
            Err(error) => {
                return Err(DbError::QueryFailure(error));
            }
        }

        Ok(congregations)
    }

    /// Remove record of user being part of a congregation
    /// TODO: Refine query to only delete where delete is checked
    /// TODO: Handle case of no rows affected
    pub async fn delete_user_cong_record(&self, user_id: u32, cong_id: u32) -> Result<(), DbError> {
        let update_query =
            sqlx::query("DELETE FROM user_cong_pair WHERE user_id = ? AND congregation_id = ?")
                .bind(user_id)
                .bind(cong_id);

        let rows_result = update_query.execute(&self.data).await;
        if let Err(error) = rows_result {
            return Err(DbError::QueryFailure(error));
        }
        Ok(())
    }

    /// TODO: Implement this, it will mark a congregation as deleted, mark all user_cong_pairs as deleted, and the actual row for the congregation in the database will be removed when someone syncs, removing their user_cong_pair and a check for any remaining user_cong_pair, if not, if cong is marked for deletion, it will be deleted
    /// TODO: Delete any remaining congregation data
    pub async fn delete_congregation(&self, cong_id: u32) -> Result<(), DbError> {
        let update_query =
            sqlx::query("UPDATE user_group_pair SET deleted = TRUE WHERE congregation_id = ?")
                .bind(cong_id);

        let rows_result = update_query.execute(&self.data).await;
        if let Err(error) = rows_result {
            return Err(DbError::QueryFailure(error));
        }

        let update_query =
            sqlx::query("UPDATE congregation SET deleted = TRUE WHERE id = ?").bind(cong_id);

        let rows_result = update_query.execute(&self.data).await;
        if let Err(error) = rows_result {
            return Err(DbError::QueryFailure(error));
        }
        Ok(())
    }

    // ------------------- USER FUNCTIONS ------------------

    /// Get users updated since a time that are in the clients congs
    /// TODO: Update? I don't think it is nessacary to send all users to the client
    pub async fn get_users(
        &self,
        last_sync_vec: u32,
        user_id: u32,
    ) -> Result<Vec<UserPublicDetails>, DbError> {
        let query =
            sqlx::query("SELECT users.id, users.firstname, users.lastname, users.updated, users.deleted FROM users INNER JOIN user_cong_pair ON user_cong_pair.user_id=users.id WHERE users.updated >= ? AND user_cong_pair.congregation_id IN (SELECT congregation_id FROM user_cong_pair WHERE user_id = ?)")
                .bind(last_sync_vec)
                .bind(user_id);

        let rows_result = query.fetch_all(&self.data).await;

        let mut users: Vec<UserPublicDetails> = vec![];

        match rows_result {
            Ok(rows) => {
                for row in rows {
                    let user_details = get_user_details(row);
                    match user_details {
                        Ok(user_details) => {
                            users.push(user_details);
                        }
                        Err(error) => return Err(DbError::InvalidRow(error)),
                    }
                }
            }
            Err(error) => {
                return Err(DbError::QueryFailure(error));
            }
        }

        Ok(users)
    }

    /// Fetch user by a given refresh token
    pub async fn fetch_user_from_refresh_token(
        &self,
        user_token: [u8; 32],
    ) -> Result<u32, DbError> {
        let user_id: u32;

        let mut hasher = Blake2b512::new();
        hasher.update(user_token);
        let token_hash = hasher.finalize();

        let get_user_id_query =
            sqlx::query("SELECT user FROM tokens WHERE token = ? AND refresh = true")
                .bind(hex::encode(token_hash));

        let rows_result = get_user_id_query.fetch_all(&self.data).await;
        match rows_result {
            Ok(rows) => {
                if rows.len() == 1 {
                    user_id = rows[0].get("user");
                } else {
                    return Err(DbError::InvalidToken(rows.len() as u32));
                }
            }
            Err(error) => {
                return Err(DbError::QueryFailure(error));
            }
        }

        Ok(user_id)
    }

    /// Fetch user by a given access token
    /// TODO: Write this
    pub async fn fetch_user_from_access_token(&self, user_token: [u8; 32]) -> Result<u32, DbError> {
        let user_id: u32;
        let get_user_id_query =
            sqlx::query("SELECT user FROM tokens WHERE token = ? AND refresh = false)")
                .bind(hex::encode(user_token));

        let rows_result = get_user_id_query.fetch_all(&self.data).await;
        match rows_result {
            Ok(rows) => {
                if rows.len() == 1 {
                    user_id = rows[0].get("user");
                } else {
                    return Err(DbError::InvalidToken(rows.len() as u32));
                }
            }
            Err(error) => {
                return Err(DbError::QueryFailure(error));
            }
        }

        Ok(user_id)
    }

    /// Check if the username and password hash match, return Ok(user_id) if they do
    /// TODO: Better error return value
    pub async fn check_user_password(&self, name: &str, pass_hash: &str) -> Result<u32, DbError> {
        let query = sqlx::query("SELECT * FROM users WHERE firstname = ? AND password = ?")
            .bind(name)
            .bind(pass_hash);
        let rows_result = query.fetch_all(&self.data).await;
        match rows_result {
            Ok(rows) => {
                if rows.len() == 1 {
                    let user_id = rows[0].get("id");
                    Ok(user_id)
                } else {
                    Err(DbError::InvalidToken(rows.len() as u32))
                }
            }
            Err(error) => Err(DbError::QueryFailure(error)),
        }
    }

    /// Given a refresh and access token, logout that user
    pub async fn logout_user(
        self,
        refresh_token: [u8; 32],
        access_token: [u8; 32],
    ) -> Result<bool, DbError> {
        // remove refresh token
        let remove_token_query =
            sqlx::query("DELETE FROM tokens WHERE token = ?").bind(hex::encode(refresh_token));
        let query_result = remove_token_query.execute(&self.data).await;

        if let Err(result) = query_result {
            return Err(DbError::QueryFailure(result));
        }

        // remove access tokens
        let insert_token_query =
            sqlx::query("DELETE FROM tokens WHERE token = ?").bind(hex::encode(access_token));
        let query_result = insert_token_query.execute(&self.data).await;

        if let Err(result) = query_result {
            return Err(DbError::QueryFailure(result));
        }
        Ok(true)
    }

    /// create new refresh token
    /// The refresh token is used to generate access tokens at the start of new sessions or every 15(?) minutes
    /// TODO: Check if token already exists
    pub async fn update_refresh_token(
        &self,
        user: u32,
        new_token: [u8; 32],
    ) -> Result<[u8; 32], DbError> {
        // hash token
        let mut hasher = Blake2b512::new();
        hasher.update(new_token);
        let token_hash = hasher.finalize();

        let insert_token_query =
            sqlx::query("INSERT INTO tokens(user, refresh, token) VALUES (?, ?, ?)")
                .bind(user)
                .bind(true)
                .bind(hex::encode(token_hash));

        let query_result = insert_token_query.execute(&self.data).await;

        if let Err(result) = query_result {
            return Err(DbError::QueryFailure(result));
        }

        Ok(new_token)
    }

    /// access token
    /// access tokens are temporary tokens only valid for 15(?) minutes after creation
    /// TODO: Check if token already exists
    pub async fn update_access_token(
        &self,
        user: u32,
        new_token: [u8; 32],
    ) -> Result<[u8; 32], DbError> {
        // hash token
        let mut hasher = Blake2b512::new();
        hasher.update(new_token);
        let token_hash = hasher.finalize();

        let insert_token_query =
            sqlx::query("INSERT INTO tokens(user, refresh, token) VALUES (?, ?, ?)")
                // TODO: Something wrong with user id so foreign key constraint fails?
                .bind(user)
                .bind(false)
                .bind(hex::encode(token_hash));

        let query_result = insert_token_query.execute(&self.data).await;

        if let Err(result) = query_result {
            return Err(DbError::QueryFailure(result));
        }

        Ok(new_token)
    }

    /// revoke all tokens from a given user in the database
    pub async fn revoke_tokens(&self, user: u32) -> Result<(), DbError> {
        let insert_token_query = sqlx::query("DELETE FROM tokens WHERE user = ?").bind(user);

        let query_result = insert_token_query.execute(&self.data).await;

        if let Err(result) = query_result {
            return Err(DbError::QueryFailure(result));
        }
        Ok(())
    }

    // ------------------ CATEGORY FUNCTIONS -----------------

    /// Get all categories updated after a timestamp
    /// TODO: update to only fetch categories related to passed congregations
    pub async fn get_categories(&self, last_sync: u32) -> Result<Vec<CategoryDetails>, DbError> {
        let get_maps_query =
            sqlx::query("SELECT * FROM categories WHERE updated >= ?").bind(last_sync);

        let rows_result = get_maps_query.fetch_all(&self.data).await;

        match rows_result {
            Ok(result) => {
                let mut categories = vec![];
                for row in result {
                    let row_to_details = category_row_to_details(row);
                    match row_to_details {
                        Ok(category_details) => categories.push(category_details),
                        Err(error) => {
                            return Err(DbError::InvalidRow(error));
                        }
                    }
                }
                Ok(categories)
            }
            Err(error) => Err(DbError::QueryFailure(error)),
        }
    }

    // ------------------ GROUP FUNCTIONS -----------------

    // Get all groups for a user
    pub async fn get_groups(&self, user_id: u32) -> Result<Vec<GroupDetails>, DbError> {
        let query = sqlx::query("SELECT user_group_pair.group_id AS group_id, user_group_pair.deleted AS pair_deleted, user_group_pair.updated AS pair_updated, service_group.name AS name, service_group.elder AS elder, service_group.deleted AS group_deleted, service_group.updated AS group_updated, service_group.congregation AS congregation FROM user_group_pair INNER JOIN service_group ON service_group.id=user_group_pair.group_id WHERE user_id = ?")
        .bind(user_id);

        let rows_result = query.fetch_all(&self.data).await;

        let mut groups: Vec<GroupDetails> = vec![];

        match rows_result {
            Ok(rows) => {
                for row in rows {
                    let group_details = get_group_details(row);
                    match group_details {
                        Ok(details) => {
                            groups.push(details);
                        }
                        Err(error) => return Err(DbError::InvalidRow(error)),
                    }
                }
            }
            Err(error) => {
                return Err(DbError::QueryFailure(error));
            }
        }

        Ok(groups)
    }

    // Remove record of user being part of a group
    // TODO: Refine query to only delete where a row is marked for deletion
    // TODO: Handle edge case of no rows removed
    pub async fn delete_user_group_record(
        &self,
        user_id: u32,
        group_id: u32,
    ) -> Result<(), DbError> {
        let update_query =
            sqlx::query("DELETE FROM user_group_pair WHERE user_id = ? AND group_id = ?")
                .bind(user_id)
                .bind(group_id);

        let rows_result = update_query.execute(&self.data).await;
        if let Err(error) = rows_result {
            return Err(DbError::QueryFailure(error));
        }
        Ok(())
    }

    pub async fn delete_group_record(&self, group_id: u32) -> Result<(), DbError> {
        let query =
            sqlx::query("SELECT user_id FROM user_group_pair WHERE group_id = ?").bind(group_id);

        let rows_result = query.fetch_all(&self.data).await;

        match rows_result {
            Ok(rows) => {
                if rows.is_empty() {
                    let update_query =
                        sqlx::query("DELETE FROM service_group WHERE id = ?").bind(group_id);

                    let rows_result = update_query.execute(&self.data).await;
                    if let Err(error) = rows_result {
                        return Err(DbError::QueryFailure(error));
                    }
                }
            }
            Err(error) => {
                return Err(DbError::QueryFailure(error));
            }
        }

        Ok(())
    }

    // ------------------ MAP FUNCTIONS -----------------

    /// Get maps for a user
    /// TODO: Restrict based on maps visible to user, not congregation
    pub async fn get_maps(&self, user_id: u32, last_sync: u32) -> Result<Vec<MapDetails>, DbError> {
        let get_maps_query =
            sqlx::query("SELECT * FROM maps WHERE updated >= ? AND congregation_id IN (SELECT congregation_id FROM user_cong_pair WHERE user_id = ?)")
                .bind(last_sync)
                .bind(user_id);

        let rows_result = get_maps_query.fetch_all(&self.data).await;

        let mut maps = vec![];

        match rows_result {
            Ok(rows) => {
                for row in rows {
                    let map_details = get_map_details(row);
                    match map_details {
                        Ok(map_details) => maps.push(map_details),
                        Err(error) => return Err(DbError::InvalidRow(error)),
                    }
                }
            }
            Err(error) => return Err(DbError::QueryFailure(error)),
        }

        Ok(maps)
    }

    // TODO: be more selective
    pub async fn get_streets(&self) -> Result<Vec<StreetDetails>, DbError> {
        let query = sqlx::query("SELECT * FROM streets");

        let rows_result = query.fetch_all(&self.data).await;

        let mut streets: Vec<StreetDetails> = vec![];

        match rows_result {
            Ok(rows) => {
                for row in rows {
                    let street_details = street_row_to_details(row);
                    match street_details {
                        Ok(user_details) => {
                            streets.push(user_details);
                        }
                        Err(error) => return Err(DbError::InvalidRow(error)),
                    }
                }
            }
            Err(error) => {
                return Err(DbError::QueryFailure(error));
            }
        }

        Ok(streets)
    }

    // TODO: be more selective
    pub async fn get_addresses(&self) -> Result<Vec<AddressDetails>, DbError> {
        let query = sqlx::query("SELECT * FROM addresses");

        let rows_result = query.fetch_all(&self.data).await;

        let mut users: Vec<AddressDetails> = vec![];

        match rows_result {
            Ok(rows) => {
                for row in rows {
                    let user_details = get_address_details(row);
                    match user_details {
                        Ok(user_details) => {
                            users.push(user_details);
                        }
                        Err(error) => return Err(DbError::AddressFailure(error)),
                    }
                }
            }
            Err(error) => {
                return Err(DbError::QueryFailure(error));
            }
        }

        Ok(users)
    }

    pub async fn complete_address(&self, address_id: u32, checked: bool) -> Result<(), DbError> {
        let update_query = sqlx::query("UPDATE addresses SET visited = ? WHERE id = ?")
            .bind(checked)
            .bind(address_id);

        let rows_result = update_query.execute(&self.data).await;
        if let Err(error) = rows_result {
            return Err(DbError::QueryFailure(error));
        }
        Ok(())
    }
}

// ---------------- Helper functions --------------

/// Convert a row of the congregations table to the CongDetails datatype
fn cong_row_to_details(row: SqliteRow) -> Result<CongDetails, sqlx::Error> {
    let cong_id: u32 = row.try_get("congregation_id")?;
    let cong_name: String = row.try_get("name")?;
    let remove: bool = row.try_get("deleted")?;
    let updated: u32 = row.try_get("updated")?;
    Ok(CongDetails {
        cong_id,
        cong_name,
        remove,
        updated,
    })
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
/// TODO: have a remove variable rather than something hard coded
fn category_row_to_details(row: SqliteRow) -> Result<CategoryDetails, sqlx::Error> {
    let id = row.try_get("id")?;
    let name = row.try_get("name")?;
    let prefix = row.try_get("prefix")?;
    let congregation = row.try_get("congregation")?;
    let updated = row.try_get("updated")?;
    let remove = false;
    Ok(CategoryDetails {
        id,
        name,
        prefix,
        congregation,
        remove,
        updated,
    })
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
    let group_updated: u32 = row.try_get("group_updated")?;
    let pair_updated: u32 = row.try_get("pair_updated")?;
    let updated: u32 = if group_updated > pair_updated {
        group_updated
    } else {
        pair_updated
    };
    let group_deleted: bool = row.try_get("group_deleted")?;
    let pair_deleted: bool = row.try_get("pair_deleted")?;
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

/// TODO: Write docs
fn get_user_details(row: SqliteRow) -> Result<UserPublicDetails, sqlx::Error> {
    let id = row.try_get("id")?;
    let firstname: String = row.try_get("firstname")?;
    let lastname: String = row.try_get("lastname")?;
    let deleted: bool = row.try_get("deleted")?;
    let name = format!("{} {}", firstname, lastname);
    Ok(UserPublicDetails { id, name, deleted })
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
    let id = row.try_get("id")?;
    let name = row.try_get("name")?;
    let image_name: String = row.try_get("file_name")?;
    let assignee: u32 = row.try_get("assignee")?;
    let assigner: u32 = row.try_get("assigner")?;
    let category: u32 = row.try_get("category")?;
    let deleted = row.try_get("deleted")?;
    Ok(MapDetails {
        id,
        name,
        image_name,
        assignee,
        assigner,
        image: None,
        category,
        deleted,
    })
}

fn street_row_to_details(row: SqliteRow) -> Result<StreetDetails, sqlx::Error> {
    let id = row.try_get("id")?;
    let map_id = row.try_get("map_id")?;
    let name = row.try_get("name")?;
    let deleted = row.try_get("deleted")?;
    Ok(StreetDetails {
        id,
        map_id,
        name,
        deleted,
    })
}

fn get_address_details(row: SqliteRow) -> Result<AddressDetails, AddressError> {
    let id = row.try_get("id")?;
    let street_id = row.try_get("street_id")?;
    let number = row.try_get("number")?;
    let tags_serialised: Vec<u8> = row.try_get("tags")?;
    let tags = if !tags_serialised.is_empty() {
        rmp_serde::from_slice(&tags_serialised)?
    } else {
        vec![]
    };
    let visited: bool = row.try_get("visited")?;
    let deleted = row.try_get("deleted")?;
    Ok(AddressDetails {
        id,
        street_id,
        number,
        tags,
        visited,
        deleted,
    })
}
