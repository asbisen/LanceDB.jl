# LanceDB.jl

> **Work in progress — experimental.** The API is unstable and subject to
> breaking changes without notice. Not yet registered in the Julia General
> registry. Use at your own risk.

A pure Julia wrapper for [LanceDB](https://lancedb.com/) built on top of the official [lancedb-c](https://github.com/lancedb/lancedb-c) C FFI layer. It exposes a Tables.jl-compatible interface so query results work directly with DataFrames.jl, CSV.jl, and the rest of the Julia data ecosystem.

---

## Getting started

See **[QUICKSTART.md](https://github.com/asbisen/LanceDB.jl/blob/main/QUICKSTART.md)** for a step-by-step walkthrough you can follow from the Julia REPL by copy-pasting each block in sequence.

---

## Prerequisites

### 1. Rust toolchain

The C library is written in Rust. Install `rustup` if you don't have it:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Ensure the stable toolchain is active:

```bash
rustup default stable
```

### 2. CMake ≥ 3.20

| Platform | Install |
|---|---|
| macOS | `brew install cmake` |
| Ubuntu/Debian | `sudo apt install cmake` |

### 3. Julia ≥ 1.10

Download from [julialang.org](https://julialang.org/downloads/).

---

## Building the shared library

The Julia package loads `liblancedb.so` (Linux) or `liblancedb.dylib`
(macOS) at startup. You must build it from source before using the package.

### Clone lancedb-c

```bash
git clone https://github.com/lancedb/lancedb-c.git
```

### Build (Linux)

```bash
cd lancedb-c/build
cmake ..
make -j$(nproc)
```

The shared library is produced at:

```
lancedb-c/build/target/release/liblancedb.so
```

### Build (macOS)

```bash
cd lancedb-c/build
cmake ..
make -j$(sysctl -n hw.logicalcpu)
```

The shared library is produced at:

```
lancedb-c/build/target/release/liblancedb.dylib
```

---

## Running the Julia package

Clone this repository alongside the `lancedb-c` directory so they share the same parent folder (the default library path is relative):

```
parent/
├── lancedb-c/          ← cloned above
│   └── build/target/release/liblancedb.so
└── julia-lance/        ← this repository
    └── LanceDB.jl/
```

Start Julia in the package directory:

```bash
cd julia-lance/LanceDB.jl
julia --project=.
```

```julia
using LanceDB
using Tables

conn = Connection("/tmp/my_lancedb")
println(table_names(conn))   # []
```

If `liblancedb` lives somewhere else, set the environment variable before loading the package:

```bash
LANCEDB_LIB=/path/to/liblancedb.so julia --project=.
```

### Run the tests

```bash
julia --project=. test/runtests.jl
```

---

## Repository layout

```
lancedb-c/          C FFI library (build this first)
lancedb-rs/         Exploratory Rust examples
LanceDB.jl/         Julia package
  src/              Package source
  test/             Test suite
  QUICKSTART.md     End-to-end usage guide
```

---

## License

See [lancedb-c/LICENSE](lancedb-c/LICENSE) for the upstream C library. The Julia wrapper in `LanceDB.jl/` is released under the Apache 2.0 License.
