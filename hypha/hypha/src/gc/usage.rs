//! Physical cache usage and backend compaction.
//!
//! Missing or failed measurements disable eviction rather than guessing at pressure; debris
//! reclaim continues independently.

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use hypha_core::config;
use hypha_core::error::{Error, Result};

#[derive(Clone, Copy, Debug)]
pub(super) struct Usage {
    pub(super) used: u64,
    pub(super) capacity: u64,
}

impl Usage {
    /// Unknown capacity reads as empty rather than full: a backend that cannot say how large it is
    /// must not be read as an emergency.
    pub(super) fn ratio(&self) -> f64 {
        if self.capacity == 0 {
            return 0.0;
        }
        self.used as f64 / self.capacity as f64
    }

    /// Bytes that have to go for usage to reach `mark`.
    pub(super) fn excess_over(&self, mark: f64) -> u64 {
        let target = (self.capacity as f64 * mark) as u64;
        self.used.saturating_sub(target)
    }
}

#[async_trait]
pub(super) trait UsageSource: Send + Sync {
    async fn sample(&self) -> Result<Usage>;

    /// Reclaim dead bytes inside the backend, before anything live is evicted — the same
    /// zero-rehydration-risk trade debris reclaim makes (rung 0). Backends with no such notion
    /// implement it as a no-op.
    async fn compact(&self) -> Result<()>;
}

pub(super) fn connect(cfg: &config::Usage) -> std::sync::Arc<dyn UsageSource> {
    match cfg {
        config::Usage::Seaweedfs {
            master,
            garbage_threshold,
        } => std::sync::Arc::new(SeaweedFs::new(master.clone(), *garbage_threshold)),
    }
}

/// A ceiling on every master/volume call: a hung metrics endpoint must not hold a GC pass open past
/// the interval that would start the next one.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// SeaweedFS, read through the master's HTTP admin surface.
///
/// The master knows the *topology* but not disk bytes, so usage takes two hops: `/dir/status` names
/// the volume servers, and each server's `/status` reports its filesystem's own totals. That is the
/// physically accurate figure — it counts dead bytes, replication overhead, and the volume files'
/// slack, none of which the S3 API can see.
///
/// **The two response shapes this depends on**, and nothing else: the master's topology exposing
/// data nodes each carrying some form of address, and a volume server's `DiskStatuses` carrying
/// `all`/`used` byte counts. Every field is optional here, so a SeaweedFS that renames one degrades
/// to "usage unknown" — GC stops evicting and warns — instead of panicking or, far worse, reading a
/// missing field as an empty cache. That degradation is silent by construction, so each level of
/// the topology accepts both spellings SeaweedFS has used: an unmatched shape and an empty one are
/// the same value here, and only the alias keeps them apart.
struct SeaweedFs {
    master: String,
    garbage_threshold: f64,
    http: reqwest::Client,
}

impl SeaweedFs {
    fn new(master: String, garbage_threshold: f64) -> Self {
        SeaweedFs {
            master: master.trim_end_matches('/').to_string(),
            garbage_threshold,
            http: reqwest::Client::builder()
                .timeout(HTTP_TIMEOUT)
                .build()
                .unwrap_or_default(),
        }
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T> {
        self.http
            .get(url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| Error::Backend(format!("seaweedfs {url}: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Backend(format!("seaweedfs {url}: unexpected response: {e}")))
    }
}

#[async_trait]
impl UsageSource for SeaweedFs {
    async fn sample(&self) -> Result<Usage> {
        let status: DirStatus = self.get(&format!("{}/dir/status", self.master)).await?;
        let servers = status.volume_servers();
        if servers.is_empty() {
            return Err(Error::Backend(
                "seaweedfs topology named no volume servers".into(),
            ));
        }

        // One unreachable server would otherwise under-report the whole cluster as roomier than it
        // is, which is the direction that loses data.
        let statuses = futures::future::try_join_all(servers.iter().map(|server| {
            let url = format!("{server}/status");
            async move { self.get::<VolumeStatus>(&url).await }
        }))
        .await?;

        let mut total = Usage {
            used: 0,
            capacity: 0,
        };
        for disk in statuses.iter().flat_map(|s| &s.disk_statuses) {
            total.used += disk.used.unwrap_or(0);
            total.capacity += disk.all.unwrap_or(0);
        }
        Ok(total)
    }

    /// The master applies `garbageThreshold` per volume, so this costs nothing when nothing is dirty
    /// enough to be worth rewriting — which is what makes it safe to ask on every pressured pass.
    async fn compact(&self) -> Result<()> {
        let url = format!(
            "{}/vol/vacuum?garbageThreshold={}",
            self.master, self.garbage_threshold
        );
        self.http
            .get(&url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| Error::Backend(format!("seaweedfs vacuum: {e}")))?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct DirStatus {
    #[serde(rename = "Topology")]
    topology: Option<Topology>,
}

#[derive(Deserialize)]
struct Topology {
    #[serde(rename = "DataCenterInfos", alias = "DataCenters", default)]
    data_centers: Vec<DataCenter>,
}

#[derive(Deserialize)]
struct DataCenter {
    #[serde(rename = "RackInfos", alias = "Racks", default)]
    racks: Vec<Rack>,
}

#[derive(Deserialize)]
struct Rack {
    #[serde(rename = "DataNodeInfos", alias = "DataNodes", default)]
    nodes: Vec<DataNode>,
}

/// Which field carries a node's HTTP address has moved between SeaweedFS versions, so all three
/// spellings are accepted and the first present one wins.
#[derive(Deserialize)]
struct DataNode {
    #[serde(rename = "Url")]
    url: Option<String>,
    #[serde(rename = "PublicUrl")]
    public_url: Option<String>,
    #[serde(rename = "Id")]
    id: Option<String>,
}

impl DirStatus {
    fn volume_servers(&self) -> Vec<String> {
        self.topology
            .iter()
            .flat_map(|t| &t.data_centers)
            .flat_map(|dc| &dc.racks)
            .flat_map(|rack| &rack.nodes)
            .filter_map(DataNode::address)
            .collect()
    }
}

impl DataNode {
    fn address(&self) -> Option<String> {
        let addr = [&self.url, &self.public_url, &self.id]
            .into_iter()
            .flatten()
            .find(|a| !a.is_empty())?;
        Some(if addr.starts_with("http") {
            addr.clone()
        } else {
            format!("http://{addr}")
        })
    }
}

#[derive(Deserialize)]
struct VolumeStatus {
    #[serde(rename = "DiskStatuses", default)]
    disk_statuses: Vec<DiskStatus>,
}

#[derive(Deserialize)]
struct DiskStatus {
    all: Option<u64>,
    used: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratio_and_excess_are_taken_against_capacity() {
        let usage = Usage {
            used: 900,
            capacity: 1000,
        };
        assert_eq!(usage.ratio(), 0.9);
        assert_eq!(usage.excess_over(0.7), 200);
        assert_eq!(
            usage.excess_over(0.95),
            0,
            "a mark above current usage owes nothing"
        );
    }

    #[test]
    fn unknown_capacity_reads_as_empty() {
        let usage = Usage {
            used: 42,
            capacity: 0,
        };
        assert_eq!(usage.ratio(), 0.0);
        assert_eq!(usage.excess_over(0.7), 42);
    }

    #[test]
    fn topology_yields_absolute_volume_server_urls() {
        let status: DirStatus = serde_json::from_str(
            r#"{"Topology":{"DataCenterInfos":[{"RackInfos":[
                 {"DataNodeInfos":[{"Id":"10.0.0.1:8080"},{"Url":"http://vol-1:8080"}]}]}]}}"#,
        )
        .expect("tolerant of the fields it does not read");
        assert_eq!(
            status.volume_servers(),
            vec!["http://10.0.0.1:8080", "http://vol-1:8080"]
        );
    }

    /// The spelling SeaweedFS 4.37 actually serves; the `*Infos` names above are the older one.
    #[test]
    fn topology_reads_the_unsuffixed_spelling_too() {
        let status: DirStatus = serde_json::from_str(
            r#"{"Topology":{"DataCenters":[{"Racks":[
                 {"DataNodes":[{"Url":"vol-1:8080","PublicUrl":"vol-1:8080"}]}]}]}}"#,
        )
        .expect("the current spelling parses");
        assert_eq!(status.volume_servers(), vec!["http://vol-1:8080"]);
    }

    #[test]
    fn a_topology_missing_every_field_names_no_servers() {
        let status: DirStatus =
            serde_json::from_str(r#"{"Version":"4.07"}"#).expect("absent topology is not an error");
        assert!(status.volume_servers().is_empty());
    }

    #[test]
    fn disk_statuses_sum_across_servers() {
        let status: VolumeStatus = serde_json::from_str(
            r#"{"DiskStatuses":[{"dir":"/data","all":100,"used":40},{"all":100,"used":10}]}"#,
        )
        .unwrap();
        let used: u64 = status.disk_statuses.iter().map(|d| d.used.unwrap()).sum();
        assert_eq!(used, 50);
    }
}
