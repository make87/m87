use serde::{Deserialize, Serialize};

use crate::roles::Role;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Organization {
    pub id: String,
    pub role: Role,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Invite {
    pub id: String,
    pub email: String,
    pub org_id: String,
}

// accept/reject body
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AcceptRejectBody {
    pub invite_id: String,
    pub accepted: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateOrganizationBody {
    pub id: String,
    pub owner_email: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UpdateOrganizationBody {
    pub new_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InviteMemberBody {
    pub email: String,
    pub role: Role,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddDeviceBody {
    pub device_id: String,
    pub role: Role,
}
