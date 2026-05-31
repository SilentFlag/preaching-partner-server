// use argon2::password_hash;
use crate::datatypes::DbError;
use blake2::{Blake2b512, Digest};
use sqlx::{Pool, Row, Sqlite, SqlitePool, sqlite::SqliteConnectOptions};
use std::str::FromStr;

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

    /// Fetch user by a given refresh token
    pub(in crate::auth) async fn fetch_user_from_refresh_token(
        &self,
        user_token: [u8; 32],
    ) -> Result<u32, DbError> {
        let user_id: u32;
        let get_user_id_query =
            sqlx::query("SELECT user FROM tokens WHERE token = ? AND refresh = true)")
                .bind(hex::encode(&user_token));

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
    pub(in crate::auth) async fn fetch_user_from_access_token(
        &self,
        user_token: [u8; 32],
    ) -> Result<u32, DbError> {
        let user_id: u32;
        let get_user_id_query =
            sqlx::query("SELECT user FROM tokens WHERE token = ? AND refresh = false)")
                .bind(hex::encode(&user_token));

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
    pub(in crate::auth) async fn check_user_password(
        &self,
        name: &str,
        pass_hash: &str,
    ) -> Result<u32, bool> {
        let query = sqlx::query("SELECT * FROM users WHERE firstname = ? AND password = ?")
            .bind(&name)
            .bind(&pass_hash);
        let rows_result = query.fetch_all(&self.data).await;
        if let Ok(rows) = rows_result {
            if rows.len() == 1 {
                let user_id = rows[0].get("id");
                return Ok(user_id);
            }
        }
        Err(false)
    }

    /// Given a refresh and access token, logout that user
    pub(in crate::auth) async fn logout_user(
        self,
        refresh_token: [u8; 32],
        access_token: [u8; 32],
    ) -> Result<bool, DbError> {
        // remove refresh token
        let remove_token_query =
            sqlx::query("DELETE FROM tokens WHERE token = ?").bind(hex::encode(&refresh_token));
        let query_result = remove_token_query.execute(&self.data).await;

        if let Err(result) = query_result {
            return Err(DbError::QueryFailure(result));
        }

        // remove access tokens
        let insert_token_query =
            sqlx::query("DELETE FROM tokens WHERE token = ?").bind(hex::encode(&access_token));
        let query_result = insert_token_query.execute(&self.data).await;

        if let Err(result) = query_result {
            return Err(DbError::QueryFailure(result));
        }
        Ok(true)
    }

    /// create new refresh token
    /// The refresh token is used to generate access tokens at the start of new sessions or every 15(?) minutes
    /// TODO: Check if token already exists
    pub(in crate::auth) async fn update_refresh_token(
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
                .bind(&user)
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
    pub(in crate::auth) async fn update_access_token(
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
                .bind(&user)
                .bind(false)
                .bind(hex::encode(token_hash));

        let query_result = insert_token_query.execute(&self.data).await;

        if let Err(result) = query_result {
            return Err(DbError::QueryFailure(result));
        }

        Ok(new_token)
    }

    /// revoke all tokens from a given user in the database
    pub(in crate::auth) async fn revoke_tokens(&self, user: u32) -> Result<(), DbError> {
        let insert_token_query = sqlx::query("DELETE FROM tokens WHERE user = ?").bind(&user);

        let query_result = insert_token_query.execute(&self.data).await;

        if let Err(result) = query_result {
            return Err(DbError::QueryFailure(result));
        }
        Ok(())
    }
}
