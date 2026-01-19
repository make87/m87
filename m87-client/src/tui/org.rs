use crate::tui::helper::{Align, ColSpec, RenderOpts, Table, dim, role_badge, terminal_width};
use m87_shared::org::Organization;

pub fn print_device_organizations(orgs: &[Organization]) {
    if orgs.is_empty() {
        println!("{}", dim("No organizations found"));
        return;
    }

    let term_w = terminal_width().unwrap_or(96);
    let opts = RenderOpts::default();

    let t = Table::new(
        term_w.saturating_sub(2),
        1,
        vec![
            ColSpec {
                title: "ORG ID",
                min: 16,
                max: Some(40),
                weight: 3,
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
        ],
    );

    let mut out = String::new();
    out.push_str("  ");
    t.header(&mut out, &opts);

    for o in orgs {
        let id = &o.id;
        let role = role_badge(&o.role);

        out.push_str("  ");
        t.row(&mut out, &[id, &role], &opts);
    }

    print!("{out}");
}
