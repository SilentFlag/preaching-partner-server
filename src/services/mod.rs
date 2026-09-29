use crate::auth::account;
use crate::datatypes::{AppState, DbError, ServerMessage, ServerPayload};
use axum::extract::Path;
use axum::{
    extract::{Multipart, State},
    response::Html,
};
use csv::Reader;
use preaching_partner_server::database::MyDatabase;
// use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tinytemplate::TinyTemplate;

static CREATED_CONG_TEMPLATE: &str = include_str!("html/add_congregation_success.html");
static CREATED_GROUP_TEMPLATE: &str = include_str!("html/add_group_success.html");
static CREATED_USER_TEMPLATE: &str = include_str!("html/import_users_success.html");

#[derive(serde::Serialize)]
struct CreateGroupContext {
    id: u32,
}

// TODO: Don't return success page if something failed

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

// TODO: check for duplicates
pub async fn import_users(
    State(app_state): State<AppState>,
    Path(id): Path<u32>,
    mut payload: Multipart,
) -> Html<String> {
    let db = app_state.db;
    // TODO: validate user is logged in and has permission to import users
    while let Ok(Some(field)) = payload.next_field().await {
        let field_name = field.name().unwrap_or("unknown");
        if field_name == "file" {
            // TODO: Check file type
            let data = field.bytes().await.unwrap();
            let data_str = String::from_utf8(data.to_vec()).unwrap();
            println!("File as string: {}", data_str);
            let mut rdr = Reader::from_reader(data_str.as_bytes());
            let records = rdr.records();
            for record in records {
                match record {
                    Ok(record) => {
                        let firstname = record.get(0).unwrap_or("unknown");
                        let lastname = record.get(1).unwrap_or("unknown");
                        let congregation = id;
                        let name = format!("{} {}", firstname, lastname);
                        println!("Importing user: {}", name);
                        match db.create_user(firstname, lastname, congregation).await {
                            Ok(_) => println!("Successfully imported user"),
                            Err(error) => println!("Error importing user: {}", error),
                        }
                    }
                    Err(error) => println!("Error reading record: {}", error),
                }
            }
        }
    }
    let context = CreateGroupContext { id };
    let mut tt = TinyTemplate::new();
    tt.add_template("root", CREATED_USER_TEMPLATE).unwrap();
    let html_response = tt.render("root", &context).unwrap_or_else(|_| {
        "An unknown error occured, please refresh the page or try again in a few minutes"
            .to_string()
    });
    Html::from(html_response)
}

pub async fn add_congregation(
    State(app_state): State<AppState>,
    mut payload: Multipart,
) -> Html<String> {
    let db = app_state.db;
    while let Ok(Some(field)) = payload.next_field().await {
        let field_name = field.name().unwrap_or("unknown");
        if field_name == "name" {
            let data = field.bytes().await.unwrap();
            let name = String::from_utf8(data.to_vec()).unwrap();
            println!("Creating congregation: {}", name);
            match db.create_congregation(name.as_str()).await {
                Ok(_) => println!("Successfully created congregation"),
                Err(error) => println!("Error creating congregation: {}", error),
            }
        }
    }
    let mut tt = TinyTemplate::new();
    tt.add_template("root", CREATED_CONG_TEMPLATE).unwrap();
    let html_response = tt.render("root", &()).unwrap_or_else(|_| {
        "An unknown error occured, please refresh the page or try again in a few minutes"
            .to_string()
    });
    Html::from(html_response)
}

pub async fn add_group(
    State(app_state): State<AppState>,
    Path(id): Path<u32>,
    mut payload: Multipart,
) -> Html<String> {
    let db = app_state.db;
    while let Ok(Some(field)) = payload.next_field().await {
        let field_name = field.name().unwrap_or("unknown");
        if field_name == "name" {
            let data = field.bytes().await.unwrap();
            let name = String::from_utf8(data.to_vec()).unwrap();
            println!("Creating group: {}", name);
            match db.create_group(name.as_str(), id, 0).await {
                Ok(_) => println!("Successfully created group"),
                Err(error) => println!("Error creating group: {}", error),
            }
        }
    }
    let context = CreateGroupContext { id };
    let mut tt = TinyTemplate::new();
    tt.add_template("root", CREATED_GROUP_TEMPLATE).unwrap();
    let html_response = tt.render("root", &context).unwrap_or_else(|_| {
        "An unknown error occured, please refresh the page or try again in a few minutes"
            .to_string()
    });
    Html::from(html_response)
}
