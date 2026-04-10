pub async fn login(name: String, password: String, db: &sqlx::Pool<sqlx::Sqlite>) -> Result<bool, bool> {
    println!("{} {}", name, password);
    let query = sqlx::query("SELECT * FROM users WHERE firstname = ? AND password = ?")
        .bind(&name)
        .bind(&password);
    let rows_result = query.fetch_all(db).await;
    if let Ok(rows) = rows_result {
        Ok(rows.len() == 1)
    } else {
        return Err(false);
    }
}

pub fn roll_access_token() {

}

pub fn roll_refresh_token() {

}

pub fn revoke_tokens() {

}