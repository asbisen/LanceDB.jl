module LanceDB

using Arrow
using Tables

# ── Library path ──────────────────────────────────────────────────────────────
# Development default: relative to this file, up to julia-lance/lancedb-c/build/...
# Override by setting LANCEDB_LIB environment variable before loading the module.
# When LanceDB_jll is published, replace this with `using LanceDB_jll`.
const liblancedb = let
    from_env = get(ENV, "LANCEDB_LIB", "")
    if !isempty(from_env)
        from_env
    else
        candidate = normpath(joinpath(@__DIR__, "..", "..", "lancedb-c",
                                      "build", "target", "release", "liblancedb.so"))
        isfile(candidate) || error(
            "liblancedb.so not found at $candidate\n" *
            "Either build lancedb-c first or set the LANCEDB_LIB environment variable."
        )
        candidate
    end
end

# ── Source includes (order matters) ──────────────────────────────────────────
include("ctypes.jl")      # primitive handle types, enums, C value-type structs
include("api.jl")         # raw ccall wrappers  (PLACEHOLDER — regenerate via gen/)
include("error.jl")       # LanceDBException, check(), check_ptr()
include("arrow_abi.jl")   # ArrowSchema / ArrowArray layout + schema builders
include("arrow_data.jl")  # Tables.jl → Arrow C ABI (_to_arrow_c_abi, _make_reader)
include("connection.jl")  # Connection, open_table, create_table, drop_table
include("table.jl")       # Table, count_rows, add, delete_rows, merge_insert
include("result.jl")      # QueryResult (Tables.jl interface)
include("expr.jl")        # LanceDBExpr DSL (must precede query.jl)
include("query.jl")       # Query, VectorQuery, execute — references LanceDBExpr
include("index.jl")       # create_vector_index, create_scalar_index, etc.

# ── Exports ───────────────────────────────────────────────────────────────────

# Types
export Connection, Table, Query, VectorQuery, QueryResult, LanceDBExpr
export LanceDBException

# C config structs (users may need to construct these)
export LanceDBVectorIndexConfig, LanceDBScalarIndexConfig, LanceDBFtsIndexConfig
export LanceDBMergeInsertConfig, LanceDBSessionOptions

# Arrow C ABI helpers (for building schemas to pass to create_table)
export make_schema, make_vector_schema, release_arrow_schema, ArrowSchema

# Enums
export DistanceType, IndexType, OptimizeType
export L2, Cosine, Dot, Hamming
export Auto, BTree, Bitmap, LabelList, FTS, IVFFlat, IVFPQ, IVFHNSWpq, IVFHNSWsq
export OptimizeAll, OptimizeCompact, OptimizePrune, OptimizeIndex

# Connection operations
export uri, table_names, open_table, create_table, drop_table

# Table operations
export count_rows, table_version, delete_rows, add, merge_insert, optimize

# Query building
export query, vector_search, execute
export filter_where, filter_expr, select_cols, offset
export distance_type, nprobes, refine_factor, ef

# Expression DSL
export col, lit, isnull, isnotnull

# Index management
export create_vector_index, create_scalar_index, create_fts_index
export list_indices, drop_index, index_stats

end # module LanceDB
