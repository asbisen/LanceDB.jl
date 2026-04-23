import Base.Libc

# Converts Tables.jl data to Arrow C ABI structures for passing to lancedb-c.
#
# Memory model:
#   - Column DATA lives in Julia-heap Vectors pinned by GC.@preserve in the caller.
#   - ArrowArray/buffer-pointer HEADERS are Libc.malloc'd and freed by us after
#     lancedb_table_add/create returns (i.e. after Arrow-rs has read all data).
#   - release = C_NULL: Arrow-rs Drop skips the callback when NULL, so no
#     cross-thread @cfunction invocation is needed. Julia GC owns the data.

# ── Low-level ArrowArray builders ─────────────────────────────────────────────
# release = C_NULL: Arrow-rs FFI_ArrowArray::drop() skips the release call when
# the pointer is None/NULL, so we avoid invoking any callback from a non-Julia
# Tokio thread. Julia data is kept alive by GC.@preserve; GC frees it afterwards.

function _alloc_buf_ptrs(ptrs::Vector{Ptr{Cvoid}})::Ptr{Ptr{Cvoid}}
    n   = length(ptrs)
    buf = Ptr{Ptr{Cvoid}}(Libc.malloc(n * sizeof(Ptr{Cvoid})))
    for (i, p) in enumerate(ptrs)
        unsafe_store!(buf, p, i)
    end
    buf
end

function _alloc_child_ptrs(children::Vector{Ptr{ArrowArray}})::Ptr{Ptr{ArrowArray}}
    n   = length(children)
    n == 0 && return Ptr{Ptr{ArrowArray}}(C_NULL)
    buf = Ptr{Ptr{ArrowArray}}(Libc.malloc(n * sizeof(Ptr{ArrowArray})))
    for (i, c) in enumerate(children)
        unsafe_store!(buf, c, i)
    end
    buf
end

function _heap_array(arr::ArrowArray)::Ptr{ArrowArray}
    ptr = Ptr{ArrowArray}(Libc.malloc(sizeof(ArrowArray)))
    unsafe_store!(ptr, arr)
    ptr
end

# Primitive column — data must be pinned by caller via GC.@preserve.
function _primitive_arr(data::AbstractVector{T})::Ptr{ArrowArray} where T
    bufs = _alloc_buf_ptrs([C_NULL, Ptr{Cvoid}(pointer(data))])
    _heap_array(ArrowArray(
        Int64(length(data)), 0, 0, 2, 0,
        Ptr{Cvoid}(bufs), C_NULL, C_NULL, C_NULL, C_NULL
    ))
end

# UTF-8 string column.
# Returns (array_ptr, offsets_vec, bytes_vec); caller must GC.@preserve both vecs.
function _string_arr(data::AbstractVector)
    n       = length(data)
    offsets = Vector{Int32}(undef, n + 1)
    offsets[1] = Int32(0)
    for (i, s) in enumerate(data)
        offsets[i+1] = offsets[i] + Int32(ncodeunits(String(s)))
    end
    bytes = Vector{UInt8}(undef, offsets[end])
    pos   = 1
    for s in data
        cu = codeunits(String(s))
        nb = length(cu)
        copyto!(bytes, pos, cu, 1, nb)
        pos += nb
    end
    bufs = _alloc_buf_ptrs([C_NULL,
                             Ptr{Cvoid}(pointer(offsets)),
                             Ptr{Cvoid}(pointer(bytes))])
    arr  = _heap_array(ArrowArray(
        Int64(n), 0, 0, 3, 0,
        Ptr{Cvoid}(bufs), C_NULL, C_NULL, C_NULL, C_NULL
    ))
    arr, offsets, bytes
end

# FixedSizeList column — flat_data is pre-concatenated; caller must GC.@preserve it.
function _fixed_list_arr(flat_data::Vector{Float32}, n_rows::Int)::Ptr{ArrowArray}
    child     = _primitive_arr(flat_data)
    child_arr = _alloc_child_ptrs([child])
    bufs      = _alloc_buf_ptrs([C_NULL])
    _heap_array(ArrowArray(
        Int64(n_rows), 0, 0, 1, 1,
        Ptr{Cvoid}(bufs), Ptr{Cvoid}(child_arr), C_NULL, C_NULL, C_NULL
    ))
end

# Root struct array (record batch).
function _struct_arr(children::Vector{Ptr{ArrowArray}}, n_rows::Int)::Ptr{ArrowArray}
    child_arr = _alloc_child_ptrs(children)
    bufs      = _alloc_buf_ptrs([C_NULL])
    _heap_array(ArrowArray(
        Int64(n_rows), 0, 0, 1, length(children),
        Ptr{Cvoid}(bufs), Ptr{Cvoid}(child_arr), C_NULL, C_NULL, C_NULL
    ))
end

# ── Free ArrowArray header tree (C-allocated only; data is Julia GC'd) ────────

function _free_array_tree(ptr::Ptr{ArrowArray})
    ptr == C_NULL && return
    s = unsafe_load(ptr)
    s.buffers != C_NULL && Libc.free(s.buffers)
    if s.n_children > 0 && s.children != C_NULL
        cptr = Ptr{Ptr{ArrowArray}}(s.children)
        for i in 1:s.n_children
            child = unsafe_load(cptr, i)
            child != C_NULL && _free_array_tree(child)
        end
        Libc.free(s.children)
    end
    Libc.free(ptr)
end

# ── Schema helpers for data-derived schemas ────────────────────────────────────

function _arrow_format(T::Type)
    T === Int8    && return "c"
    T === UInt8   && return "C"
    T === Int16   && return "s"
    T === UInt16  && return "S"
    T === Int32   && return "i"
    T === UInt32  && return "I"
    T === Int64   && return "l"
    T === UInt64  && return "L"
    T === Float32 && return "f"
    T === Float64 && return "g"
    T <: AbstractString && return "u"
    error("Unsupported column type for Arrow export: $T")
end

function _fixed_list_field_schema(name::String, dim::Int)::Ptr{ArrowSchema}
    float_child  = _alloc_leaf_schema("f", "item")
    vec_children = Ptr{Ptr{ArrowSchema}}(Libc.malloc(sizeof(Ptr{Cvoid})))
    unsafe_store!(vec_children, float_child, 1)
    ptr = Ptr{ArrowSchema}(Libc.malloc(sizeof(ArrowSchema)))
    unsafe_store!(ptr, ArrowSchema(
        _malloc_cstr("+w:$dim"), _malloc_cstr(name), C_NULL,
        0, 1, Ptr{Cvoid}(vec_children), C_NULL, C_NULL, C_NULL
    ))
    ptr
end

# ── Main entry: Tables.jl → (array_ptr, schema_ptr, pins) ────────────────────

"""
    _to_arrow_c_abi(data) -> (array_ptr, schema_ptr, pins)

Convert a Tables.jl-compatible object to Arrow C ABI pointers.

- `array_ptr`  — root struct ArrowArray* for the record batch (Libc-allocated header)
- `schema_ptr` — root ArrowSchema* (Libc-allocated; release with `release_arrow_schema`)
- `pins`       — Vector{Any} of Julia objects that MUST be kept alive via
                 `GC.@preserve pins` for as long as the C pointers are in use

Supported column element types: Int8/16/32/64, UInt8/16/32/64, Float32/64,
AbstractString (→ UTF-8), and AbstractVector{Float32} (→ FixedSizeList).
"""
function _to_arrow_c_abi(data)
    cols   = Tables.columns(data)
    names  = Tables.columnnames(cols)
    isempty(names) && error("Cannot build Arrow batch from empty table (no columns)")
    n_rows = length(Tables.getcolumn(cols, first(names)))

    pins         = Any[]
    col_arrays   = Ptr{ArrowArray}[]
    col_schemas  = Ptr{ArrowSchema}[]

    for nm in names
        col   = Tables.getcolumn(cols, nm)
        T     = eltype(col)
        sname = String(nm)

        if T <: AbstractVector   # FixedSizeList<Float32>
            ET = eltype(T)
            ET === Float32 || error("FixedSizeList columns must have Float32 elements, got $ET")
            dim  = length(first(col))
            flat = Vector{Float32}(undef, n_rows * dim)
            for (i, v) in enumerate(col)
                length(v) == dim || error("Embedding length mismatch at row $i: expected $dim, got $(length(v))")
                copyto!(flat, (i - 1) * dim + 1, v, 1, dim)
            end
            push!(pins, flat)
            push!(col_arrays,  _fixed_list_arr(flat, n_rows))
            push!(col_schemas, _fixed_list_field_schema(sname, dim))

        elseif T <: AbstractString
            arr, offsets, bytes = _string_arr(col)
            push!(pins, offsets, bytes)
            push!(col_arrays, arr)
            push!(col_schemas, _alloc_leaf_schema("u", sname))

        else   # primitive numeric
            vec = collect(T, col)
            push!(pins, vec)
            push!(col_arrays,  _primitive_arr(vec))
            push!(col_schemas, _alloc_leaf_schema(_arrow_format(T), sname))
        end
    end

    # Root struct ArrowArray
    root_arr = _struct_arr(col_arrays, n_rows)

    # Root struct ArrowSchema
    n        = length(col_schemas)
    children = Ptr{Ptr{ArrowSchema}}(Libc.malloc(n * sizeof(Ptr{Cvoid})))
    for (i, f) in enumerate(col_schemas)
        unsafe_store!(children, f, i)
    end
    root_schema = Ptr{ArrowSchema}(Libc.malloc(sizeof(ArrowSchema)))
    unsafe_store!(root_schema, ArrowSchema(
        _malloc_cstr("+s"), _malloc_cstr(""), C_NULL,
        0, Int64(n), Ptr{Cvoid}(children), C_NULL, C_NULL, C_NULL
    ))

    root_arr, root_schema, pins
end

# ── Shared helper: build reader from a Tables.jl source ───────────────────────
# Returns (reader_ptr, schema_ptr, array_ptr, pins).
# The caller must:
#   1.  Call lancedb_table_add or lancedb_table_create inside GC.@preserve pins
#   2.  After that call, invoke _free_array_tree(array_ptr) and
#       release_arrow_schema(schema_ptr)

function _make_reader(data)
    arr_ptr, schema_ptr, pins = _to_arrow_c_abi(data)
    reader_out = Ref{Ptr{LanceDBRecordBatchReaderHandle}}(C_NULL)
    errmsg     = Ref{Ptr{UInt8}}(C_NULL)
    GC.@preserve pins begin
        code = lancedb_record_batch_reader_from_arrow(
            Ptr{Cvoid}(arr_ptr), Ptr{Cvoid}(schema_ptr), reader_out, errmsg)
    end
    check(code, errmsg)
    reader_out[], schema_ptr, arr_ptr, pins
end
