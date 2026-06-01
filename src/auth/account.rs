use rand::TryRng;
use rand::rngs::SysRng;

use crate::database::MyDatabase;
use crate::datatypes::DbError;

/// Check the validity of given credentials
/// If they are valid, the user id of the user is returned, Ok(id)
/// If they are invalid, Err(false) is returned
///
/// TODO: Hash password before checking database
pub async fn login(name: String, password: String, db: MyDatabase) -> Result<u32, DbError> {
    // TODO: Hash password to match a hash in the database
    db.check_user_password(&name, &password).await
}

/// Log out a single device based on its refresh token
/// If everything is successful Ok(true) is returned
/// If something is unsuccessful Err(false) is returned
///
/// TODO: Update function to be able to log out other sessions (eg, user wants to log out session on other personal device, admin wants to logout other client)
pub async fn _logout(
    refresh_token: [u8; 32],
    access_token: [u8; 32],
    db: MyDatabase,
) -> Result<bool, DbError> {
    // get user id of token
    db.logout_user(refresh_token, access_token).await
}

/// Generate a new refresh token for a given user
/// A hashed version is inserted into the tokens database table
/// Return value is an unhashed version of the token
pub async fn roll_refresh_token(user: u32, db: MyDatabase) -> Result<[u8; 32], DbError> {
    let mut rng = SysRng;
    let mut buf = [0u8; 32];
    let token_result = rng.try_fill_bytes(&mut buf);

    if let Err(token_error) = token_result {
        return Err(DbError::TokenRngFailure(token_error));
    }

    db.update_refresh_token(user, buf).await
}

/// Generate a new access token for a given user identified by refresh token
/// Return value is an unhashed version of the token
///
/// TODO: move database operations to MyDatabase
/// TODO: hndle error of token failing to generate
/// TODO: handle the error of the token failing to be entered into the database
pub async fn roll_access_token(
    refresh_token: [u8; 32],
    db: MyDatabase,
) -> Result<[u8; 32], DbError> {
    // Get id of user to be updated

    let user_id = db.fetch_user_from_refresh_token(refresh_token).await?;

    let mut rng = SysRng;
    let mut new_token = [0u8; 32];
    let token_result = rng.try_fill_bytes(&mut new_token); // TODO: handle fail of filling bytes

    if let Err(token_error) = token_result {
        return Err(DbError::TokenRngFailure(token_error));
    }

    db.update_access_token(user_id, new_token).await
}

/// Remove all tokens associated with a given user in the database
pub async fn _revoke_tokens(user: u32, db: MyDatabase) -> Result<(), DbError> {
    db.revoke_tokens(user).await
}
