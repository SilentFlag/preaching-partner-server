use axum::{extract::State, response::Html};
use preaching_partner_server::datatypes::{AppState, CongDetails, UserDetails};
use serde::Serialize;
use tinytemplate::TinyTemplate;

#[derive(Serialize)]
struct CongContext {
    table_data: Vec<CongDetails>,
}

#[derive(Serialize)]
struct UserContext {
    table_data: Vec<UserDetails>,
}

static ROOT_TEMPLATE: &str = include_str!("html/root.html");
static CONGREGATIONS_TEMPLATE: &str = include_str!("html/congregations.html");
static ADD_CONGREGATION_TEMPLATE: &str = include_str!("html/add_congregation.html");
static USERS_TEMPLATE: &str = include_str!("html/users.html");
static IMPORT_USERS_TEMPLATE: &str = include_str!("html/import_users.html");

/// TODO: handle errors, maybe with error page
pub async fn root() -> Html<std::string::String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("root", ROOT_TEMPLATE).unwrap();
    let html_response = tt.render("root", &()).unwrap_or_else(|_| {
        "An unknown error occured, please refresh the page or try again in a few minutes"
            .to_string()
    });
    Html::from(html_response)
}

pub async fn users(State(app_state): State<AppState>) -> Html<std::string::String> {
    // TODO: Handle error case
    let users = app_state.db.get_all_users().await.unwrap();
    let mut tt = TinyTemplate::new();
    tt.add_template("root", USERS_TEMPLATE).unwrap();

    let context = UserContext { table_data: users };

    let html_response = tt.render("root", &context).unwrap_or_else(|_| {
        "An unknown error occured, please refresh the page or try again in a few minutes"
            .to_string()
    });
    Html::from(html_response)
}

pub async fn import_users() -> Html<std::string::String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("import_users", IMPORT_USERS_TEMPLATE)
        .unwrap();
    let html_response = tt.render("import_users", &()).unwrap_or_else(|_| {
        "An unknown error occured, please refresh the page or try again in a few minutes"
            .to_string()
    });
    Html::from(html_response)
}

pub async fn add_congregation() -> Html<std::string::String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("add_congregation", ADD_CONGREGATION_TEMPLATE)
        .unwrap();
    let html_response = tt.render("add_congregation", &()).unwrap_or_else(|_| {
        "An unknown error occured, please refresh the page or try again in a few minutes"
            .to_string()
    });
    Html::from(html_response)
}

pub async fn congregations(State(app_state): State<AppState>) -> Html<std::string::String> {
    // TODO: Handle error case
    let congregations = app_state.db.get_all_congregations().await.unwrap();
    let mut tt = TinyTemplate::new();
    tt.add_template("root", CONGREGATIONS_TEMPLATE).unwrap();

    let context = CongContext {
        table_data: congregations,
    };

    let html_response = tt.render("root", &context).unwrap_or_else(|_| {
        "An unknown error occured, please refresh the page or try again in a few minutes"
            .to_string()
    });
    Html::from(html_response)
}
