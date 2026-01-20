use std::collections::HashSet;

use anyhow::{Result, anyhow};
use m87_shared::{
    device::PublicDevice,
    org::{Invite, Organization},
    roles::Role,
    users::User,
};

use crate::{
    auth::AuthManager, config::Config, devices::resolve_device_cached, server,
    util::servers_parallel::fanout_servers,
};

pub async fn list_organizations() -> Result<Vec<Organization>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let results = fanout_servers(config.manager_server_urls, 4, false, |server_url| {
        let token = token.clone();
        async move { server::list_organizations(&server_url, &token, trust).await }
    })
    .await?;

    // avoid duplicate organizations
    let mut out: HashSet<Organization> = HashSet::new();

    for (_, org) in results {
        out.insert(org);
    }

    Ok(out.into_iter().collect())
}

pub async fn create_organization(id: &str, owner_email: &str) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let _: Vec<_> = fanout_servers(config.manager_server_urls, 4, false, |server_url| {
        let token = token.clone();
        async move {
            server::create_organization(&server_url, &token, trust, id, owner_email).await?;
            Ok(Vec::<()>::new())
        }
    })
    .await?;

    Ok(())
}

pub async fn delete_organization(id: &str) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let _: Vec<_> = fanout_servers(config.manager_server_urls, 4, false, |server_url| {
        let token = token.clone();
        async move {
            server::delete_organization(&server_url, &token, trust, id).await?;
            Ok(Vec::<()>::new())
        }
    })
    .await?;

    Ok(())
}

pub async fn update_organization(id: &str, new_id: &str) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let _: Vec<_> = fanout_servers(config.manager_server_urls, 4, false, |server_url| {
        let token = token.clone();
        let id = id.to_string();
        let new_id = new_id.to_string();
        async move {
            server::update_organization(&server_url, &token, trust, &id, &new_id).await?;
            Ok(Vec::<()>::new())
        }
    })
    .await?;

    Ok(())
}

// list members
pub async fn list_members(id: Option<String>) -> Result<Vec<User>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let id = get_or_resolve_default_org_id(id).await?;

    let results = fanout_servers(config.manager_server_urls, 4, false, |server_url| {
        let token = token.clone();
        let id = id.clone();
        async move { server::list_organization_members(&server_url, &token, trust, id).await }
    })
    .await?;

    let mut out: HashSet<User> = HashSet::new();

    for (_, member) in results {
        out.insert(member);
    }

    Ok(out.into_iter().collect())
}

// add member
pub async fn add_member(id: Option<String>, email: &str, role: Role) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let id = get_or_resolve_default_org_id(id).await?;

    let _: Vec<_> = fanout_servers(config.manager_server_urls, 4, false, |server_url| {
        let token = token.clone();
        let id = id.clone();
        let email = email.to_string();
        let role = role.clone();
        async move {
            server::add_organization_member(&server_url, &token, trust, id, email, role).await?;
            Ok(Vec::<()>::new())
        }
    })
    .await?;

    Ok(())
}

// remove member
pub async fn remove_member(id: Option<String>, email: &str) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let id = get_or_resolve_default_org_id(id).await?;

    let _: Vec<_> = fanout_servers(config.manager_server_urls, 4, false, |server_url| {
        let token = token.clone();
        let id = id.clone();
        let email = email.to_string();
        async move {
            server::remove_organization_member(&server_url, &token, trust, id, email).await?;
            Ok(Vec::<()>::new())
        }
    })
    .await?;
    Ok(())
}

pub async fn list_invites() -> Result<Vec<Invite>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let results = fanout_servers(config.manager_server_urls, 4, false, |server_url| {
        let token = token.clone();
        async move { server::list_organization_invites(&server_url, &token, trust).await }
    })
    .await?;

    let mut out: HashSet<Invite> = HashSet::new();

    for (_, invite) in results {
        out.insert(invite);
    }

    Ok(out.into_iter().collect())
}

pub async fn handle_invite(invite_id: &str, accept: bool) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let mut config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let results: Vec<(String, String)> =
        fanout_servers(config.manager_server_urls.clone(), 4, false, |server_url| {
            let token = token.clone();
            async move {
                let org_id = server::handle_organization_invite(
                    &server_url,
                    &token,
                    trust,
                    invite_id,
                    accept,
                )
                .await?;
                Ok(vec![org_id])
            }
        })
        .await?;

    let org_ids = results
        .into_iter()
        .map(|(_, response)| Ok(response))
        .collect::<Result<Vec<String>>>()?;

    if accept {
        // save first ok org id into config.organization_id
        config.organization_id = Some(org_ids[0].clone());
        config.save()?;
    }

    Ok(())
}

pub async fn list_devices(org_id: Option<String>) -> Result<Vec<PublicDevice>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let org_id = get_or_resolve_default_org_id(org_id).await?;

    let results = fanout_servers(config.manager_server_urls, 4, false, |server_url| {
        let token = token.clone();
        let org_id = org_id.clone();
        async move { server::list_org_devices(&server_url, &token, trust, &org_id).await }
    })
    .await?;

    let mut out = Vec::new();

    for (_, devices) in results {
        out.push(devices);
    }

    Ok(out)
}

pub async fn add_device(org_id: Option<String>, device_name: &str, role: Role) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let resolved = resolve_device_cached(device_name).await?;

    let org_id = get_or_resolve_default_org_id(org_id).await?;

    let _: Vec<_> = fanout_servers(config.manager_server_urls, 4, false, |server_url| {
        let token = token.clone();
        let device_id = resolved.id.clone();
        let org_id = org_id.clone();
        let role = role.clone();
        async move {
            server::add_org_device(&server_url, &token, trust, &org_id, &device_id, role).await?;
            Ok(Vec::<()>::new())
        }
    })
    .await?;
    Ok(())
}

pub async fn remove_device(org_id: Option<String>, device_name: &str) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let resolved = resolve_device_cached(device_name).await?;

    let org_id = get_or_resolve_default_org_id(org_id).await?;

    let _: Vec<_> = fanout_servers(config.manager_server_urls, 4, false, |server_url| {
        let token = token.clone();
        let device_id = resolved.id.clone();
        let org_id = org_id.clone();
        async move {
            server::remove_org_device(&server_url, &token, trust, &org_id, &device_id).await?;
            Ok(Vec::<()>::new())
        }
    })
    .await?;
    Ok(())
}

pub async fn get_or_resolve_default_org_id(org_id: Option<String>) -> Result<String> {
    let mut config = Config::load()?;

    match (org_id, config.organization_id) {
        (Some(id), _) => Ok(id),
        (None, Some(id)) => Ok(id),
        (None, None) => {
            let orgs = list_organizations().await?;
            if orgs.is_empty() {
                Err(anyhow!("No organizations found"))
            } else {
                // take first store inconfig save then return
                let org_id = orgs[0].id.clone();
                config.organization_id = Some(org_id.clone());
                config.save()?;
                Ok(org_id)
            }
        }
    }
}
