use crate::tui::helper::{Align, ColSpec, RenderOpts, Table, dim, role_badge, terminal_width};

use m87_shared::users::User;

pub fn print_users(users: &[User]) {
    if users.is_empty() {
        println!("{}", dim("No users found"));
        return;
    }

    let term_w = terminal_width().unwrap_or(96);
    let opts = RenderOpts::default();

    let t = Table::new(
        term_w.saturating_sub(2),
        1,
        vec![
            ColSpec {
                title: "EMAIL",
                min: 22,
                max: Some(48),
                weight: 4,
                align: Align::Left,
                wrap: false,
            },
            ColSpec {
                title: "ROLE",
                min: 8,
                max: Some(10),
                weight: 0,
                align: Align::Left,
                wrap: false,
            },
            ColSpec {
                title: "ID",
                min: 8,
                max: Some(20),
                weight: 1,
                align: Align::Left,
                wrap: false,
            },
        ],
    );

    let mut out = String::new();
    out.push_str("  ");
    t.header(&mut out, &opts);

    for u in users {
        let email = &u.email;
        let role = role_badge(&u.role);
        let id = dim(&u.id);

        out.push_str("  ");
        t.row(&mut out, &[email, &role, &id], &opts);
    }

    print!("{out}");
}
