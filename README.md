# microcrab-core

An embeddable agent session runtime written in Rust. It provides session-owned
execution, permissions, conversation state, durable history, model transport and
WebAssembly agents.

## Workspace

| Area | Responsibility |
| --- | --- |
| [`session`](crates/session) | Session lifecycle, context, budgets, execution and policy |
| [`creator`](crates/creator), [`storage`](crates/storage) | Session assembly, persistence and recovery |
| [`transport`](crates/transport) | OpenAI-compatible, OpenAI Responses, Anthropic and Google model transport |
| [`host`](crates/host) | Host interfaces, local/remote implementations and browser source resources |
| [`agent`](crates/agent) | Shared agent types, WIT contracts, Coding and Universal WASM agents |
| [`shared`](crates/shared), [`auditor`](crates/auditor) | Protocol, permission and audit contracts |
| [`xtask`](crates/xtask) | Architecture checks and WASM agent builds |

Server applications, the web UI and Python bindings are maintained separately.

## Use

Add the packages required by your application as Git dependencies:

```toml
[dependencies]
microcrab-session = { git = "https://github.com/Ky1eYang/microcrab-core", branch = "main" }
microcrab-host-local = { git = "https://github.com/Ky1eYang/microcrab-core", branch = "main" }
```

Use the same Git URL and revision for all core packages. Commit your application's
lockfile; use a fixed `rev` when an explicitly pinned source revision is required.
Embedding applications configure their Host, model transport, permissions and
tracing subscriber.

## Development

Use a recent stable Rust toolchain supporting edition 2024 and a native build
environment for your platform. The Windows configuration expects `rust-lld.exe`
on `PATH`; SQLite-enabled builds also require the Windows SDK.

```sh
git clone git@github.com:Ky1eYang/microcrab-core.git
cd microcrab-core
cargo fetch --locked
cargo fmt --all -- --check
cargo xtask check-session
cargo test --workspace --all-features --locked --offline -- --test-threads=1
```

Routine checks use the committed lockfile and cached dependencies. Tests that
explicitly require prepared guest components are opt-in and identify their
prerequisites in their ignore messages.

To build and validate WASM agents, first install `wasm-tools` and the
`wasm32-unknown-unknown` Rust target, then run:

```sh
cargo xtask build-wasm --release --validate --install
```

Components are installed under `.microcrab/agents` relative to the calling working
directory. Use `--install-dir PATH` to choose another deployment directory.
Browser Host JavaScript sources are included; packaged browser runtimes and UI
assets are prepared by their consuming application.

## Contributing

Open an issue or pull request with a focused change and relevant verification.
Preserve Session ownership boundaries, permission checks and frozen history.
Keep credentials, private configuration and generated artifacts out of commits.

## License

[MIT](LICENSE).
