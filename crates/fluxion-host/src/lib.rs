pub mod scheduler;

use anyhow::Result;
use std::path::Path;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};

wasmtime::component::bindgen!({
    path: "../../wit/task.wit",
    world: "task-component",
});

struct HostState {
    ctx: WasiCtx,
    table: ResourceTable,
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

    pub fn run_component(&self, wasm_path: impl AsRef<Path>, input: Vec<u8>) -> Result<Vec<u8>> {
        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker)?;

        let table = ResourceTable::new();
        let wasi = WasiCtxBuilder::new()
            .inherit_stdout()
            .inherit_stderr()
            .build();
        let state = HostState { ctx: wasi, table };
        let mut store = Store::new(&self.engine, state);

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
