use axum::{
    extract::{Path, State},
    response::Html,
};
use preaching_partner_server::datatypes::{AppState, CongDetails, GroupDetails};
use serde::Serialize;
use tinytemplate::TinyTemplate;

#[derive(Serialize)]
struct CongContext {
    table_data: Vec<CongDetails>,
}

#[derive(Serialize)]
struct CongregationDetailsContext {
    data: CongDetails,
    groups: Vec<GroupDetails>,
}

#[derive(Serialize)]
struct IdContext {
    id: u32,
}

static ROOT_TEMPLATE: &str = include_str!("html/root.html");
static CONGREGATION_DETAILS_TEMPLATE: &str = include_str!("html/congregation_details.html");
static ADD_CONGREGATION_TEMPLATE: &str = include_str!("html/add_congregation.html");
static ADD_GROUPS_TEMPLATE: &str = include_str!("html/add_group.html");
static IMPORT_USERS_TEMPLATE: &str = include_str!("html/import_users.html");

/// TODO: handle errors, maybe with error page
pub async fn root(State(app_state): State<AppState>) -> Html<std::string::String> {
    let congregations = app_state.db.get_all_congregations().await.unwrap();
    let mut tt = TinyTemplate::new();
    tt.add_template("root", ROOT_TEMPLATE).unwrap();

    let context = CongContext {
        table_data: congregations,
    };

    let html_response = tt.render("root", &context).unwrap_or_else(|_| {
        "An unknown error occured, please refresh the page or try again in a few minutes"
            .to_string()
    });
    Html::from(html_response)
}

// CONGREGATIONS

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

pub async fn congregation_details(
    State(app_state): State<AppState>,
    Path(id): Path<u32>,
) -> Html<std::string::String> {
    let cong_details = app_state.db.get_congregation_details(id).await.unwrap();
    let groups = app_state.db.get_groups_by_congregation(id).await.unwrap();
    println!("Congregation details: {:?}", groups);
    let mut tt = TinyTemplate::new();
    tt.add_template("congregation_details", CONGREGATION_DETAILS_TEMPLATE)
        .unwrap();

    let context = CongregationDetailsContext {
        data: cong_details,
        groups,
    };
    let html_response = tt
        .render("congregation_details", &context)
        .unwrap_or_else(|e| {
            eprintln!("Error rendering congregation details: {}", e);
            "An unknown error occured, please refresh the page or try again in a few minutes"
                .to_string()
        });
    Html::from(html_response)
}

// GROUPS

pub async fn add_group(Path(id): Path<u32>) -> Html<std::string::String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("add_group", ADD_GROUPS_TEMPLATE).unwrap();
    let context = IdContext { id };
    let html_response = tt.render("add_group", &context).unwrap_or_else(|_| {
        "An unknown error occured, please refresh the page or try again in a few minutes"
            .to_string()
    });
    Html::from(html_response)
}

// USERS

pub async fn import_users(Path(id): Path<u32>) -> Html<std::string::String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("import_users", IMPORT_USERS_TEMPLATE)
        .unwrap();
    let context = IdContext { id };
    let html_response = tt.render("import_users", &context).unwrap_or_else(|_| {
        "An unknown error occured, please refresh the page or try again in a few minutes"
            .to_string()
    });
    Html::from(html_response)
}
