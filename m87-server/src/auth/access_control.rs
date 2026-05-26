use mongodb::bson::{Document, doc};

/// Sentinel scope inserted into a `Claims`'s scope list to signal "no access
/// restriction" — used by the admin-key auth path. `access_filter` checks for
/// this value and returns an empty document so the resulting query matches
/// every record. Picked to be a string no real user/org reference could
/// collide with (real scopes are ObjectId hex strings).
pub const ADMIN_WILDCARD_SCOPE: &str = "__admin_wildcard__";

/// Trait for any Mongo model that has a scope-like field controlling access.

pub trait AccessControlled {
    fn owner_scope_field() -> &'static str;
    // optional field
    fn allowed_scopes_field() -> Option<&'static str>;

    fn access_filter(scopes: &Vec<String>) -> Document {
        // Admin bypass: an admin claim carries the wildcard scope, which
        // means "no scope restriction" — return an empty filter that matches
        // every document.
        if scopes.iter().any(|s| s == ADMIN_WILDCARD_SCOPE) {
            return doc! {};
        }
        // if allowed scopes is none dont add it to the filter
        if let Some(field) = Self::allowed_scopes_field() {
            doc! {
                "$or": [
                    { Self::owner_scope_field(): { "$in": scopes } },
                    { field: { "$in": scopes } }
                ]
            }
        } else {
            doc! {
                Self::owner_scope_field(): { "$in": scopes }
            }
        }
    }

    fn owner_scope(&self) -> &str;
    fn allowed_scopes(&self) -> Option<Vec<String>>;
}
