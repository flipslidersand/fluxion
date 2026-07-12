pub mod scheduler;

use anyhow::{Context, Result};
use fluxion_core::workflow::PermissionSet;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store, StoreLimitsBuilder};
use wasmtime_wasi::{DirPerms, FilePerms, ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};

wasmtime::component::bindgen!({
    path: "../../wit/task.wit",
    world: "task-component",
});

struct HostState {
    ctx: WasiCtx,
    table: ResourceTable,
    limits: wasmtime::StoreLimits,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.ctx
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

pub struct FluxionHost {
    engine: Engine,
}

impl FluxionHost {
    pub fn new() -> Result<Self> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        let engine = Engine::new(&config)?;
        Ok(Self { engine })
    }

    pub fn run_component(
        &self,
        wasm_path: impl AsRef<Path>,
        input: Vec<u8>,
        perms: &PermissionSet,
    ) -> Result<Vec<u8>> {
        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker)?;

        let ctx = build_wasi_ctx(perms)?;
        let limits = StoreLimitsBuilder::new()
            .memory_size(perms.limits.memory_mb as usize * 1024 * 1024)
            .build();

        let state = HostState { ctx, table: ResourceTable::new(), limits };
        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| &mut s.limits);

        let component = Component::from_file(&self.engine, wasm_path)?;
        let instance = TaskComponent::instantiate(&mut store, &component, &linker)?;

        let task_input = exports::fluxion::task::processor::TaskInput {
            content: input,
            metadata: vec![],
        };

        let result = instance
            .fluxion_task_processor()
            .call_process(&mut store, &task_input)?;

        match result {
            Ok(output) => Ok(output.content),
            Err(e) => anyhow::bail!("Component error: {}", e),
        }
    }
}

// An entry in the network allowlist: either an exact IP:port or all ports on an IP.
#[derive(Debug)]
enum NetworkEntry {
    Exact(SocketAddr),
    AnyPort(IpAddr),
}

impl NetworkEntry {
    fn matches(&self, addr: SocketAddr) -> bool {
        match self {
            Self::Exact(a) => *a == addr,
            Self::AnyPort(ip) => *ip == addr.ip(),
        }
    }
}

fn parse_network_entry(s: &str) -> Option<NetworkEntry> {
    if let Ok(a) = s.parse::<SocketAddr>() {
        return Some(NetworkEntry::Exact(a));
    }
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(NetworkEntry::AnyPort(ip));
    }
    None
}

fn build_wasi_ctx(perms: &PermissionSet) -> Result<WasiCtx> {
    let mut builder = WasiCtxBuilder::new();
    builder.inherit_stdout().inherit_stderr();

    // Filesystem: preopen read dirs
    for path in &perms.filesystem.read {
        if path.exists() {
            let guest = path.to_string_lossy().to_string();
            builder.preopened_dir(path, &guest, DirPerms::READ, FilePerms::READ)?;
        }
    }

    // Filesystem: preopen read-write dirs (created on demand)
    for path in &perms.filesystem.write {
        std::fs::create_dir_all(path)
            .with_context(|| format!("Failed to create write dir {:?}", path))?;
        let guest = path.to_string_lossy().to_string();
        builder.preopened_dir(
            path,
            &guest,
            DirPerms::READ | DirPerms::MUTATE,
            FilePerms::READ | FilePerms::WRITE,
        )?;
    }

    // Network capability gate.
    // SocketAddrCheck::default() already returns false for every address, so
    // deny-all requires no extra work. We only install a check when an explicit
    // allowlist is provided.
    if !perms.network.allow.is_empty() {
        let entries: Vec<NetworkEntry> = perms.network.allow
            .iter()
            .filter_map(|s| parse_network_entry(s))
            .collect();

        anyhow::ensure!(
            !entries.is_empty(),
            "network.allow has entries but none could be parsed as `IP` or `IP:port`"
        );

        // ip_name_lookup is false by default; we keep DNS off since the
        // allowlist is IP-based. Callers must specify resolved IPs.
        builder.socket_addr_check(move |addr, _use| {
            let ok = entries.iter().any(|e| e.matches(addr));
            Box::pin(async move { ok })
        });
    }

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_exact_addr() {
        let e = parse_network_entry("93.184.216.34:443").unwrap();
        assert!(matches!(e, NetworkEntry::Exact(_)));
    }

    #[test]
    fn parse_ip_only() {
        let e = parse_network_entry("93.184.216.34").unwrap();
        assert!(matches!(e, NetworkEntry::AnyPort(_)));
    }

    #[test]
    fn parse_invalid_returns_none() {
        assert!(parse_network_entry("example.com:443").is_none());
        assert!(parse_network_entry("not-an-addr").is_none());
    }

    #[test]
    fn exact_entry_matches_only_same_port() {
        let e = NetworkEntry::Exact("93.184.216.34:443".parse().unwrap());
        assert!(e.matches("93.184.216.34:443".parse().unwrap()));
        assert!(!e.matches("93.184.216.34:80".parse().unwrap()));
        assert!(!e.matches("1.2.3.4:443".parse().unwrap()));
    }

    #[test]
    fn any_port_entry_matches_all_ports() {
        let e = NetworkEntry::AnyPort("93.184.216.34".parse().unwrap());
        assert!(e.matches("93.184.216.34:443".parse().unwrap()));
        assert!(e.matches("93.184.216.34:80".parse().unwrap()));
        assert!(!e.matches("1.2.3.4:443".parse().unwrap()));
    }

    #[test]
    fn ipv6_exact_entry() {
        let e = parse_network_entry("[::1]:8080").unwrap();
        assert!(matches!(e, NetworkEntry::Exact(_)));
        assert!(e.matches("[::1]:8080".parse().unwrap()));
        assert!(!e.matches("[::1]:9090".parse().unwrap()));
    }
}
