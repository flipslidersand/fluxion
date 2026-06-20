# Fluxion

Safe Wasm-based job execution engine with capability-controlled sandboxing and DAG scheduling.

Each job runs as a WebAssembly component — isolated, permission-scoped, and language-agnostic.

## Required Tools

- Rust 1.82+
- `cargo-component`: `cargo install cargo-component`
- `wasm-tools`: `cargo install wasm-tools`
- `wasm32-wasip2` target: `rustup target add wasm32-wasip2`

## Setup

```bash
git clone https://github.com/flipslidersand/fluxion
cd fluxion
```

## Build

```bash
# Build the hello sample component
cargo component build --manifest-path components/hello/Cargo.toml

# Build the CLI
cargo build
```

## Run

```bash
./target/debug/fluxion component run \
  target/wasm32-wasip1/debug/hello.wasm --input "test"
# → Hello from Fluxion! Received 4 bytes.
```

## Directory Structure

```
fluxion/
├── crates/
│   ├── fluxion-cli/     # CLI binary (fluxion command)
│   ├── fluxion-core/    # Core types and logic
│   └── fluxion-host/    # Wasmtime host — loads and runs Wasm components
├── components/
│   └── hello/           # Sample Wasm component (Rust → wasm32-wasip1)
├── wit/
│   └── task.wit         # WIT interface: fluxion:task/processor
└── docs/
    ├── spec.md
    ├── tech-stack.md
    ├── data-model.md
    ├── implementation-guide.md
    └── adr/             # Architecture Decision Records
```

## Status

| Phase   | Description                   | Status     |
| ------- | ----------------------------- | ---------- |
| Phase 1 | Wasm component runtime + CLI  | ✅ Done    |
| Phase 2 | YAML workflow + DAG scheduler | 🔲 Next    |
| Phase 3 | Capability sandbox            | 🔲 Planned |
| Phase 4 | Persistence + retry           | 🔲 Planned |
| Phase 5 | OpenTelemetry observability   | 🔲 Planned |
| Phase 6 | MCP / AI integration          | 🔲 Planned |

## License

MIT
