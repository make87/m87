use mongodb::bson::{doc, Document};

/// Trait for any Mongo model that has a scope-like field controlling access.

pub trait AccessControlled {
    fn owner_scope_field() -> &'static str;
    // optional field
    fn allowed_scopes_field() -> Option<&'static str>;

    fn access_filter(scopes: &Vec<String>) -> Document {
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
}
