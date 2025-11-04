use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Owner,
    Admin,
    Editor,
    Viewer,
}

impl Role {
    pub fn allows(have: &Role, need: &Role) -> bool {
        use Role::*;
        match (have, need) {
            (Owner, _) => true,
            (Admin, Admin) | (Admin, Editor) | (Admin, Viewer) => true,
            (Editor, Editor) | (Editor, Viewer) => true,
            (Viewer, Viewer) => true,
            _ => false,
        }
    }
}
