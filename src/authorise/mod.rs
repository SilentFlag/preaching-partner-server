use crate::datatypes::ClientAction;

pub fn check_permissions(_token: Vec<u8>, _actions: Vec<ClientAction>) -> Result<bool, ()> {
    Ok(false)
}
