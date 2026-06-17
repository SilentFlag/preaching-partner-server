use crate::auth::account;
use crate::datatypes::{DbError, ServerMessage, ServerPayload};
use preaching_partner_server::database::MyDatabase;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn login_attempt(name: String, password: String, db: MyDatabase) -> Result<Vec<u8>, ()> {
    println!("Attempting login");
    let login_attempt: Result<u32, DbError> = account::login(name, password, db.clone()).await;
    let login_detail = match login_attempt {
        Ok(result) => {
            println!("succeedded login");
            let refresh_token = account::roll_refresh_token(result, db.clone()).await;
            match refresh_token {
                Ok(refresh_token) => {
                    let access_token = account::roll_access_token(refresh_token, db.clone()).await;
                    match access_token {
                        Ok(access_token) => Some((true, Some(refresh_token), Some(access_token))),
                        // TODO: Handle error
                        Err(error) => {
                            println!("Error happened at access token: {}", error);
                            None
                        }
                    }
                }
                // TODO: Handle error
                Err(_) => {
                    println!("database error with refresh token");
                    None
                }
            }
        }
        Err(_) => Some((false, None, None)),
    };

    // TODO: handle the error of time going before UNIX_EPOCH, set time to 0?
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as u32;
    let message = ServerMessage {
        id: 0,
        timestamp,
        payload: match login_detail {
            Some(details) => {
                let (success, refresh_token, access_token) = details;
                ServerPayload::ConfirmLogin {
                    success,
                    refresh_token,
                    access_token,
                }
            }
            None => ServerPayload::UnknownError,
        },
    };
    let message_bytes = rmp_serde::to_vec(&message).unwrap();
    Ok(message_bytes)
}
