## Regenerating C bindings

`src/api.jl` is auto-generated from the lancedb-c C header using
[Clang.jl](https://github.com/JuliaInterop/Clang.jl). When a new version
of lancedb-c is released you may need to rebuild the shared library **and**
regenerate the bindings to pick up any new or changed API functions.

### Steps

**1. Build the new shared library** using the steps in the section above.

**2. Copy the updated header into the generator directory:**

```bash
cp lancedb-c/include/lancedb.h LanceDB.jl/gen/lancedb.h
```

**3. Instantiate the generator's own environment** (only needed once):

```bash
julia --project=LanceDB.jl/gen -e "using Pkg; Pkg.instantiate()"
```

**4. Run the generator:**

```bash
julia --project=LanceDB.jl/gen LanceDB.jl/gen/generator.jl
```

This overwrites `LanceDB.jl/src/api.jl` with fresh `ccall` wrappers for
every function in the header.

**5. Review the diff** before committing — new API functions may need
hand-written high-level wrappers in the appropriate `src/` module, and
removed functions should be cleaned up from those modules too.

> **Note:** `src/arrow_abi.jl` is maintained by hand to match the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) spec and is **not** touched by the generator.