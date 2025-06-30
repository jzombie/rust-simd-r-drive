# SIMD R Drive Python WebSocket Client

[![made-with-rust](https://img.shields.io/badge/Made%20with-Rust-black?logo=Rust)](https://www.rust-lang.org)
[![built-with-maturin](https://img.shields.io/badge/Built%20with-maturin-orange)](https://github.com/PyO3/maturin)
[![python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org)

> **Work in progress.** Prototype Python bindings that speak the
> SIMD R Drive RPC protocol over WebSockets.

---

Prototype WebSocket bindings for [`SIMD R Drive`](https://crates.io/crates/simd-r-drive),
a high‑performance, append‑only, single‑file storage engine written in Rust.
The bindings are implemented in Rust (via [PyO3](https://github.com/PyO3/pyo3)) and packaged with
[maturin](https://github.com/PyO3/maturin).

## Requirements

| Component               | Minimum               | Notes                                                                                              |
| ----------------------- | --------------------- | -------------------------------------------------------------------------------------------------- |
| **Python**              | 3.10                  | CPython only                                                                                       |
| **SIMD R Drive Server** | matching commit       | [https://crates.io/crates/simd-r-drive-ws-server](https://crates.io/crates/simd-r-drive-ws-server) |
| **OS**                  | Linux, macOS, Windows | 64‑bit only                                                                                        |

## Installation (wheel)

```bash
pip install simd-r-drive-ws-client
```

Or build from source (Rust toolchain and `maturin` required):

```bash
pip install maturin
maturin develop --release -m experiments/bindings/python-ws-client/Cargo.toml
```

See the [CI build recipe](https://github.com/jzombie/rust-simd-r-drive/blob/main/.github/workflows/python-net-release.yml) for additional information.

## Quick Start

```python
from simd_r_drive_ws_client import DataStoreWsClient

client = DataStoreWsClient("127.0.0.1", 34129)
client.write(b"hello", b"world")
print(b"hello" in client)          # __contains__ → True
print(len(client))                 # number of active keys
print(client.read(b"hello"))       # b"world"
```

See the [type stubs](https://github.com/jzombie/rust-simd-r-drive/blob/main/experiments/bindings/python-ws-client/simd_r_drive_ws_client/data_store_ws_client.pyi)
for the full API surface.


## License

Licensed under the [Apache-2.0 License](https://github.com/jzombie/rust-simd-r-drive/blob/main/experiments/bindings/python-ws-client/LICENSE).
