use m87_shared::deploy_spec::DeploymentRevision;

pub fn print_revision_list_header() {
    println!("{:<36} {:>4} {:>8}", "REVISION", "JOBS", "ROLLBACK");
}

pub fn print_revision_short(rev: &DeploymentRevision) {
    println!(
        "{:<36} {:>4} {:>8}",
        rev.id.as_deref().unwrap_or("<none>"),
        rev.jobs.len(),
        if rev.rollback.is_some() { "yes" } else { "no" }
    );
}

pub fn print_revision_list_short(revs: &[DeploymentRevision]) {
    print_revision_list_header();
    for rev in revs {
        print_revision_short(rev);
    }
}

pub fn print_revision_verbose(rev: &DeploymentRevision) {
    match rev.to_yaml() {
        Ok(yaml) => print!("{yaml}"),
        Err(e) => eprintln!("failed to serialize revision to yaml: {e}"),
    }
}

pub fn print_revision_short_detail(rev: &DeploymentRevision) {
    // print header
    println!(
        "{:<36} {:>8} {:>8} {:>8} {:>8}",
        "JOB ID", "ENABLED", "STEPS", "OBSERVE", "FILES"
    );
    for job in &rev.jobs {
        println!(
            "  {:<36} {:>8} {:>8} {:>8} {:>8}",
            job.id,
            job.enabled,
            job.steps.len(),
            job.observe.is_some(),
            job.files.len()
        );
    }
}
