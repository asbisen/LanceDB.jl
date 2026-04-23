"""
    QueryResult

Holds a completed LanceDB query result. Implements the `Tables.jl` interface
so it integrates directly with DataFrames.jl, CSV.jl, etc.

Data is materialised on first access via the Arrow C Data Interface.
"""
mutable struct QueryResult
    handle::Ptr{LanceDBQueryResultHandle}
    _columns::Union{Nothing, NamedTuple}
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
    isnothing(qr._columns) && _materialize!(qr)
    qr._columns
end

function Tables.schema(qr::QueryResult)
    isnothing(qr._columns) && _materialize!(qr)
    cols = qr._columns
    isempty(cols) && return Tables.Schema(Symbol[], Type[])
    Tables.Schema(collect(keys(cols)), collect(eltype(v) for v in values(cols)))
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
    qr.handle = C_NULL   # consumed by lancedb_query_result_to_arrow

    n_batches  = Int(count_out[])
    schema_ptr = schema_out[]
    arrays_ptr = arrays_out[]

    qr._columns = _import_arrow_batches(schema_ptr, arrays_ptr, n_batches)

    # Free C-side allocations (all data has been copied into Julia vectors)
    lancedb_free_arrow_arrays(arrays_ptr, Csize_t(n_batches))
    lancedb_free_arrow_schema(schema_ptr)
    nothing
end

# ── Arrow C ABI → Julia ───────────────────────────────────────────────────────

function _fmt_to_type(fmt::String)
    fmt == "c" && return Int8
    fmt == "C" && return UInt8
    fmt == "s" && return Int16
    fmt == "S" && return UInt16
    fmt == "i" && return Int32
    fmt == "I" && return UInt32
    fmt == "l" && return Int64
    fmt == "L" && return UInt64
    fmt == "f" && return Float32
    fmt == "g" && return Float64
    error("Unsupported Arrow format for result import: $fmt")
end

# Read one column from an ArrowArray given its Arrow format string.
# All column data is copied into fresh Julia vectors.
#
# NOTE: Julia Ptr{T} + n advances by n *bytes*, not n elements.
# Use unsafe_load(ptr, i) for element-indexed access (1-indexed, sizeof(T)-aware).
function _read_column(arr::ArrowArray, fmt::String)
    n      = Int(arr.length)
    offset = Int(arr.offset)
    bufs   = Ptr{Ptr{Cvoid}}(arr.buffers)

    if fmt == "u"   # UTF-8 string: buffers = [validity, Int32-offsets, bytes]
        off_ptr  = Ptr{Int32}(unsafe_load(bufs, 2))
        byte_ptr = Ptr{UInt8}(unsafe_load(bufs, 3))
        result   = Vector{String}(undef, n)
        for i in 1:n
            # unsafe_load(ptr, i) is 1-indexed and sizeof-aware
            start_b = Int(unsafe_load(off_ptr, i + offset))
            end_b   = Int(unsafe_load(off_ptr, i + offset + 1))
            result[i] = unsafe_string(byte_ptr + start_b, end_b - start_b)
        end
        return result

    elseif startswith(fmt, "+w:")   # FixedSizeList<Float32>: 1 buffer + 1 child
        dim      = parse(Int, fmt[4:end])
        cptr     = Ptr{Ptr{ArrowArray}}(arr.children)
        child    = unsafe_load(unsafe_load(cptr, 1))
        c_offset = Int(child.offset)
        c_bufs   = Ptr{Ptr{Cvoid}}(child.buffers)
        data_ptr = Ptr{Float32}(unsafe_load(c_bufs, 2))
        result   = Vector{Vector{Float32}}(undef, n)
        for i in 1:n
            vec    = Vector{Float32}(undef, dim)
            estart = c_offset + (offset + i - 1) * dim + 1   # 1-indexed start for unsafe_load
            for j in 1:dim
                vec[j] = unsafe_load(data_ptr, estart + j - 1)
            end
            result[i] = vec
        end
        return result

    else   # primitive numeric: buffers = [validity, data]
        T        = _fmt_to_type(fmt)
        data_ptr = Ptr{T}(unsafe_load(bufs, 2))
        result   = Vector{T}(undef, n)
        for i in 1:n
            result[i] = unsafe_load(data_ptr, i + offset)   # 1-indexed, sizeof-aware
        end
        return result
    end
end

# Read one Arrow struct batch into a NamedTuple of column vectors.
function _read_batch_columns(batch_ptr::Ptr{Cvoid}, schema_ptr::Ptr{Cvoid})
    arr    = unsafe_load(Ptr{ArrowArray}(batch_ptr))
    schema = unsafe_load(Ptr{ArrowSchema}(schema_ptr))
    ncols  = Int(schema.n_children)
    child_schemas = Ptr{Ptr{ArrowSchema}}(schema.children)
    child_arrays  = Ptr{Ptr{ArrowArray}}(arr.children)
    names  = Vector{Symbol}(undef, ncols)
    cols   = Vector{Any}(undef, ncols)
    for i in 1:ncols
        cs       = unsafe_load(unsafe_load(child_schemas, i))
        ca       = unsafe_load(unsafe_load(child_arrays, i))
        names[i] = Symbol(unsafe_string(cs.name))
        cols[i]  = _read_column(ca, unsafe_string(cs.format))
    end
    NamedTuple{Tuple(names)}(Tuple(cols))
end

# Convert all C Arrow batches into a single NamedTuple of concatenated columns.
function _import_arrow_batches(schema_ptr::Ptr{Cvoid}, arrays_ptr::Ptr{Ptr{Cvoid}}, n::Int)
    n == 0 && return NamedTuple()
    batches = [_read_batch_columns(unsafe_load(arrays_ptr, i), schema_ptr) for i in 1:n]
    n == 1 && return batches[1]
    nms  = keys(batches[1])
    cols = [reduce(vcat, getfield.(batches, nm)) for nm in nms]
    NamedTuple{nms}(Tuple(cols))
end
