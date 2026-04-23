using Tables

"""
    QueryResult

Holds a completed LanceDB query result. Implements the `Tables.jl` interface
so it integrates directly with DataFrame.jl, CSV.jl, etc.

Data is materialized lazily on first access.
"""
mutable struct QueryResult
    handle::Ptr{LanceDBQueryResultHandle}
    _batches::Union{Nothing, Vector}   # Vector of Arrow record batches once materialized
end

QueryResult(handle::Ptr{LanceDBQueryResultHandle}) = QueryResult(handle, nothing)

function Base.close(qr::QueryResult)
    qr.handle == C_NULL && return
    lancedb_query_result_free(qr.handle)
    qr.handle = C_NULL
    nothing
end

# ── Tables.jl interface ───────────────────────────────────────────────────────

Tables.istable(::QueryResult)      = true
Tables.columnaccess(::QueryResult) = true

function Tables.columns(qr::QueryResult)
    isnothing(qr._batches) && _materialize!(qr)
    # Combine all batches into a single Arrow table
    Arrow.Table(qr._batches)
end

function Tables.schema(qr::QueryResult)
    isnothing(qr._batches) && _materialize!(qr)
    Tables.schema(Arrow.Table(qr._batches))
end

# ── Materialisation ───────────────────────────────────────────────────────────

function _materialize!(qr::QueryResult)
    qr.handle == C_NULL && throw(LanceDBException(Int32(LANCEDB_RUNTIME), "QueryResult already closed"))

    arrays_out = Ref{Ptr{Ptr{Cvoid}}}(C_NULL)
    schema_out = Ref{Ptr{Cvoid}}(C_NULL)
    count_out  = Ref{Csize_t}(0)
    errmsg     = Ref{Ptr{UInt8}}(C_NULL)

    code = lancedb_query_result_to_arrow(qr.handle, arrays_out, schema_out, count_out, errmsg)
    check(code, errmsg)
    # Handle is consumed by lancedb_query_result_to_arrow
    qr.handle = C_NULL

    n_batches  = Int(count_out[])
    schema_ptr = schema_out[]
    arrays_ptr = arrays_out[]

    batches = _import_arrow_batches(schema_ptr, arrays_ptr, n_batches)

    # Free the C-side allocations (data has been copied into Julia Arrow structures)
    lancedb_free_arrow_arrays(arrays_ptr, Csize_t(n_batches))
    lancedb_free_arrow_schema(schema_ptr)

    qr._batches = batches
    nothing
end

# Convert C Arrow ABI batches to Arrow.jl RecordBatches using Arrow IPC round-trip.
# TODO (M5): Replace with direct Arrow C Data Interface import once Arrow.jl
# exposes a public C ABI import API, avoiding the IPC serialisation overhead.
function _import_arrow_batches(schema_ptr::Ptr{Cvoid}, arrays_ptr::Ptr{Ptr{Cvoid}}, n::Int)
    # Fallback: write each batch to Arrow IPC and read back with Arrow.jl
    io = IOBuffer()
    # For each batch, reconstruct a NamedTuple-based table via the ArrowArray/Schema
    # NOTE: This is a placeholder — direct import via C ABI import is the
    # target implementation. For now we raise a clear error if someone tries
    # to materialise results before M5 is implemented.
    if n == 0
        return []
    end
    error("QueryResult materialisation requires M5 implementation. " *
          "The C ABI import path from lancedb_query_result_to_arrow is not yet wired up.")
end
