use anyhow::Result;
use futures::{StreamExt, stream};
use std::{future::Future, time::Duration};

pub async fn fanout_servers<T, F, Fut>(
    server_urls: Vec<String>,
    concurrency: usize,
    f: F,
) -> Result<Vec<(String, T)>>
where
    F: Fn(String) -> Fut + Copy,
    Fut: Future<Output = Result<Vec<T>>>,
{
    let concurrency = concurrency.max(1); // avoid buffer_unordered(0) deadlock

    let per_server_results: Vec<Result<Vec<(String, T)>>> = stream::iter(server_urls)
        .map(|server_url| {
            let url_for_items = server_url.clone();
            async move {
                tracing::debug!(server_url = %server_url, "fanout: starting");

                // pick a sane timeout for your use-case
                let res = tokio::time::timeout(Duration::from_secs(10), f(server_url)).await;

                let mapped = match res {
                    Ok(Ok(items)) => Ok(items
                        .into_iter()
                        .map(|item| (url_for_items.clone(), item))
                        .collect::<Vec<_>>()),
                    Ok(Err(e)) => Err(e),
                    Err(_) => {
                        tracing::warn!(server_url = %url_for_items, "fanout: timeout; skipping");
                        // if you want to keep signature `Result<...>`, turn timeout into an error
                        // otherwise you can return Ok(vec![]) here and skip logging later.
                        return Ok(Vec::new());
                    }
                };

                tracing::debug!(server_url = %url_for_items, ok = mapped.is_ok(), "fanout: finished");
                mapped
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut out = Vec::new();
    for res in per_server_results {
        match res {
            Ok(mut v) => out.append(&mut v),
            Err(err) => {
                tracing::warn!(error = %err, "fanout: request failed; skipping");
            }
        }
    }

    Ok(out)
}

pub async fn find_on_servers<T, F, Fut>(
    server_urls: Vec<String>,
    concurrency: usize,
    f: F,
) -> Result<Option<(String, T)>>
where
    F: Fn(String) -> Fut + Copy,
    Fut: Future<Output = Result<Option<T>>>,
{
    let mut stream = stream::iter(server_urls)
        .map(|server_url| {
            let server_url_clone = server_url.clone();
            async move {
                let res = f(server_url).await?;
                Ok::<Option<(String, T)>, anyhow::Error>(res.map(|val| (server_url_clone, val)))
            }
        })
        .buffer_unordered(concurrency);

    while let Some(res) = stream.next().await {
        if let Some(found) = res? {
            return Ok(Some(found));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fanout_servers_single_server() {
        let servers = vec!["http://server1".to_string()];
        let result = fanout_servers(servers, 2, |_url| async { Ok(vec![1, 2, 3]) }).await;

        let items = result.unwrap();
        assert_eq!(items.len(), 3);
        assert!(items.iter().all(|(url, _)| url == "http://server1"));
    }

    #[tokio::test]
    async fn test_fanout_servers_multiple_servers() {
        let servers = vec!["http://server1".to_string(), "http://server2".to_string()];
        let result = fanout_servers(servers, 2, |url| async move {
            if url == "http://server1" {
                Ok(vec!["a"])
            } else {
                Ok(vec!["b", "c"])
            }
        })
        .await;

        let items = result.unwrap();
        assert_eq!(items.len(), 3);
    }

    #[tokio::test]
    async fn test_fanout_servers_empty_results() {
        let servers = vec!["http://server1".to_string()];
        let result: Result<Vec<(String, i32)>> =
            fanout_servers(servers, 2, |_url| async { Ok(vec![]) }).await;

        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_find_on_servers_finds_first() {
        let servers = vec!["http://server1".to_string(), "http://server2".to_string()];
        // With concurrency=1, this is deterministic
        let result = find_on_servers(servers, 1, |url| async move {
            if url == "http://server1" {
                Ok(Some(42))
            } else {
                Ok(None)
            }
        })
        .await;

        let found = result.unwrap().unwrap();
        assert_eq!(found, ("http://server1".to_string(), 42));
    }

    #[tokio::test]
    async fn test_find_on_servers_none_found() {
        let servers = vec!["http://server1".to_string()];
        let result: Result<Option<(String, i32)>> =
            find_on_servers(servers, 2, |_url| async { Ok(None) }).await;

        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_find_on_servers_propagates_error() {
        let servers = vec!["http://server1".to_string()];
        let result: Result<Option<(String, i32)>> =
            find_on_servers(servers, 2, |_url| async { Err(anyhow::anyhow!("error")) }).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_find_on_servers_empty_list() {
        let servers: Vec<String> = vec![];
        let result: Result<Option<(String, i32)>> =
            find_on_servers(servers, 2, |_url| async { Ok(Some(1)) }).await;

        assert!(result.unwrap().is_none());
    }
}
