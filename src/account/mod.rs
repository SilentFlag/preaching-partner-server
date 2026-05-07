use blake2::{Blake2b512, Digest};
use hex;
use rand::TryRng;
use rand::rngs::SysRng;
use sqlx::Row;

/// Check the validity of given credentials
/// If they are valid, the user id of the user is returned, Ok(id)
/// If they are invalid, Err(false) is returned
pub async fn login(
    name: String,
    password: String,
    db: &sqlx::Pool<sqlx::Sqlite>,
) -> Result<u32, bool> {
    // TODO: Hash password to match a hash in the database
    let query = sqlx::query("SELECT * FROM users WHERE firstname = ? AND password = ?")
        .bind(&name)
        .bind(&password);
    let rows_result = query.fetch_all(db).await;
    if let Ok(rows) = rows_result {
        if rows.len() == 1 {
            let user_id = rows[0].get("id");
            return Ok(user_id);
        }
    }
    return Err(false);
}

/// Log out a single device based on its refresh token
/// If everything is successful Ok(true) is returned
/// If something is unsuccessful Err(false) is returned
///
/// TODO: Update return values to have errors relating to each possible error
pub async fn _logout(refresh_token: [u8; 32], db: &sqlx::Pool<sqlx::Sqlite>) -> Result<bool, bool> {
    // get user id of token
    let mut user_id: u32 = 0;
    let get_user_id_query =
        sqlx::query("SELECT user FROM tokens WHERE token = ? AND refresh = true)")
            .bind(hex::encode(&refresh_token));

    let rows_result = get_user_id_query.fetch_all(db).await;
    if let Ok(rows) = rows_result {
        if rows.len() == 1 {
            user_id = rows[0].get("user");
        } else {
            // This is caused by a currupt refresh token
            return Err(false);
        }
    }

    // remove refresh token
    let insert_token_query =
        sqlx::query("DELETE FROM tokens WHERE token = ?").bind(hex::encode(&refresh_token));
    let query_result = insert_token_query.execute(db).await;

    if let Err(result) = query_result {
        println!(
            "Something went wrong logging out refresh token from database, rows affected: {:?}",
            result
        );
        return Err(false);
    }

    // remove access tokens
    let insert_token_query =
        sqlx::query("DELETE FROM tokens WHERE user = ? AND refresh = false").bind(&user_id);
    let query_result = insert_token_query.execute(db).await;

    if let Err(result) = query_result {
        println!(
            "Something went wrong loggin out access tokens from database, rows affected: {:?}",
            result
        );
        return Err(false);
    }

    Ok(true)
}

/// Generate a new refresh token for a given user
/// A hashed version is inserted into the tokens database table
/// Return value is an unhashed version of the token
///
/// TODO: handle error of token failing to generate
/// TODO: handle the error of the token failing to be entered into the database
pub async fn roll_refresh_token(user: u32, db: &sqlx::Pool<sqlx::Sqlite>) -> [u8; 32] {
    let mut rng = SysRng;
    let mut buf = [0u8; 32];
    let _ = rng.try_fill_bytes(&mut buf); // TODO: handle fail of filling bytes

    // hash token
    let mut hasher = Blake2b512::new();
    hasher.update(buf);
    let token_hash = hasher.finalize();

    let insert_token_query =
        sqlx::query("INSERT INTO tokens(user, refresh, token) VALUES (?, ?, ?)")
            .bind(&user)
            .bind(true)
            .bind(hex::encode(token_hash));

    let query_result = insert_token_query.execute(db).await;

    if let Err(result) = query_result {
        // TODO: handle this error
        println!(
            "Something went wrong inserting refresh token into database, error: {:?}",
            result
        )
    }

    buf
}

/// Generate a new access token for a given user identified by refresh token
/// Return value is an unhashed version of the token
///
/// TODO: hndle error of token failing to generate
/// TODO: handle the error of the token failing to be entered into the database
pub async fn roll_access_token(
    refresh_token: [u8; 32],
    db: &sqlx::Pool<sqlx::Sqlite>,
) -> Option<[u8; 32]> {
    let mut rng = SysRng;
    let mut buf = [0u8; 32];
    let _ = rng.try_fill_bytes(&mut buf); // TODO: handle fail of filling bytes

    let mut user_id: u32 = 0;
    let get_user_id_query =
        sqlx::query("SELECT user FROM tokens WHERE token = ? AND refresh = true)")
            .bind(hex::encode(&refresh_token));

    let rows_result = get_user_id_query.fetch_all(db).await;
    if let Ok(rows) = rows_result {
        if rows.len() == 1 {
            user_id = rows[0].get("user");
        } else {
            // This is caused by a currupt refresh token
            return None;
        }
    }

    // hash token
    let mut hasher = Blake2b512::new();
    hasher.update(buf);
    let token_hash = hasher.finalize();

    let insert_token_query =
        sqlx::query("INSERT INTO tokens(user, refresh, token) VALUES (?, ?, ?)")
            // TODO: Something wrong with user id so foreign key constraint fails?
            .bind(&user_id)
            .bind(false)
            .bind(hex::encode(token_hash));

    let query_result = insert_token_query.execute(db).await;

    if let Err(result) = query_result {
        // TODO: handle this error
        println!(
            "Something went wrong inserting access token into database, error: {:?}",
            result
        )
    }

    Some(buf)
}

/// Remove all tokens associated with a given user in the database
pub async fn _revoke_tokens(user: u32, db: &sqlx::Pool<sqlx::Sqlite>) -> Result<bool, bool> {
    let insert_token_query = sqlx::query("DELETE FROM tokens WHERE user = ?").bind(&user);

    let query_result = insert_token_query.execute(db).await;

    if let Err(result) = query_result {
        println!(
            "Something went wrong revoking tokens from database, rows affected: {:?}",
            result
        );
        return Err(false);
    }
    Ok(true)
}
