pub mod scheduler;

use anyhow::Result;
use fluxion_core::workflow::PermissionSet;
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

    // Filesystem: preopen read-write dirs
    for path in &perms.filesystem.write {
        if path.exists() {
            let guest = path.to_string_lossy().to_string();
            builder.preopened_dir(
                path,
                &guest,
                DirPerms::READ | DirPerms::MUTATE,
                FilePerms::READ | FilePerms::WRITE,
            )?;
        }
    }

    Ok(builder.build())
}
