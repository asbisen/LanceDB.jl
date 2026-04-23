import Base.Libc

# Arrow C Data Interface support.
#
# Defines the concrete memory layout of FFI_ArrowSchema and FFI_ArrowArray so
# that Julia code can construct schemas/batches to pass to lancedb-c, and
# read result batches returned by lancedb-c.
#
# These structs match the Arrow C Data Interface specification exactly.

# ── Struct layouts ────────────────────────────────────────────────────────────

struct ArrowSchema
    format::Ptr{UInt8}        # const char* — Arrow format string
    name::Ptr{UInt8}          # const char* — field name (may be NULL)
    metadata::Ptr{UInt8}      # const char* — key-value metadata (may be NULL)
    flags::Int64              # field flags (e.g. nullable)
    n_children::Int64         # number of child schemas
    children::Ptr{Cvoid}      # ArrowSchema** children
    dictionary::Ptr{Cvoid}    # ArrowSchema* dictionary (for dict-encoded)
    release::Ptr{Cvoid}       # release callback (NULL for Julia-owned schemas)
    private_data::Ptr{Cvoid}
end

struct ArrowArray
    length::Int64
    null_count::Int64
    offset::Int64
    n_buffers::Int64
    n_children::Int64
    buffers::Ptr{Cvoid}       # const void**
    children::Ptr{Cvoid}      # ArrowArray** children
    dictionary::Ptr{Cvoid}    # ArrowArray* dictionary
    release::Ptr{Cvoid}       # release callback
    private_data::Ptr{Cvoid}
end

# ── Memory helpers ────────────────────────────────────────────────────────────

function _malloc_cstr(s::AbstractString)::Ptr{UInt8}
    n   = ncodeunits(s)
    ptr = Ptr{UInt8}(Libc.malloc(n + 1))
    GC.@preserve s unsafe_copyto!(ptr, pointer(codeunits(s)), n)
    unsafe_store!(ptr, 0x00, n + 1)
    ptr
end

# ── Schema construction ───────────────────────────────────────────────────────
# All Julia-constructed schemas use C_NULL for the release field.
# lancedb_table_create uses Schema::try_from(&*schema) — a reference — so it
# does NOT call release. We free memory ourselves in release_arrow_schema.

function _alloc_leaf_schema(format::String, name::String)::Ptr{ArrowSchema}
    ptr = Ptr{ArrowSchema}(Libc.malloc(sizeof(ArrowSchema)))
    unsafe_store!(ptr, ArrowSchema(
        _malloc_cstr(format), _malloc_cstr(name), C_NULL,
        0, 0, C_NULL, C_NULL, C_NULL, C_NULL
    ))
    ptr
end

"""
    make_schema(fields) -> Ptr{ArrowSchema}

Allocate a root Arrow C ABI schema (struct type) from a list of
`(name, format_string)` pairs. Caller is responsible for releasing it
via `release_arrow_schema`.

# Common format strings
- `"u"`      — UTF-8 string
- `"l"`      — Int64
- `"i"`      — Int32
- `"f"`      — Float32
- `"g"`      — Float64
- `"+w:N"`   — FixedSizeList of N elements (add a child for the element type)
- `"+s"`     — Struct (set children manually)
"""
function make_schema(fields::Vector{Pair{String,String}})::Ptr{ArrowSchema}
    n        = length(fields)
    children = Ptr{Ptr{ArrowSchema}}(Libc.malloc(n * sizeof(Ptr{Cvoid})))
    for (i, (fname, fmt)) in enumerate(fields)
        unsafe_store!(children, _alloc_leaf_schema(fmt, fname), i)
    end
    root = Ptr{ArrowSchema}(Libc.malloc(sizeof(ArrowSchema)))
    unsafe_store!(root, ArrowSchema(
        _malloc_cstr("+s"), _malloc_cstr(""), C_NULL,
        0, Int64(n), Ptr{Cvoid}(children), C_NULL, C_NULL, C_NULL
    ))
    root
end

"""
    make_vector_schema(key_field, vec_field, dim) -> Ptr{ArrowSchema}

Convenience builder for the canonical LanceDB test schema:
`{key_field: utf8, vec_field: FixedSizeList<Float32>[dim]}`.
"""
function make_vector_schema(key_field::String, vec_field::String, dim::Int)::Ptr{ArrowSchema}
    float_child  = _alloc_leaf_schema("f", "")
    vec_children = Ptr{Ptr{ArrowSchema}}(Libc.malloc(sizeof(Ptr{Cvoid})))
    unsafe_store!(vec_children, float_child, 1)
    vec_schema   = Ptr{ArrowSchema}(Libc.malloc(sizeof(ArrowSchema)))
    unsafe_store!(vec_schema, ArrowSchema(
        _malloc_cstr("+w:$dim"), _malloc_cstr(vec_field), C_NULL,
        0, 1, Ptr{Cvoid}(vec_children), C_NULL, C_NULL, C_NULL
    ))

    key_schema   = _alloc_leaf_schema("u", key_field)

    children     = Ptr{Ptr{ArrowSchema}}(Libc.malloc(2 * sizeof(Ptr{Cvoid})))
    unsafe_store!(children, key_schema, 1)
    unsafe_store!(children, vec_schema, 2)

    root = Ptr{ArrowSchema}(Libc.malloc(sizeof(ArrowSchema)))
    unsafe_store!(root, ArrowSchema(
        _malloc_cstr("+s"), _malloc_cstr(""), C_NULL,
        0, 2, Ptr{Cvoid}(children), C_NULL, C_NULL, C_NULL
    ))
    root
end

# ── Memory release ────────────────────────────────────────────────────────────

function _free_schema_recursive(ptr::Ptr{ArrowSchema})
    s = unsafe_load(ptr)
    s.format   != C_NULL && Libc.free(s.format)
    s.name     != C_NULL && Libc.free(s.name)
    s.metadata != C_NULL && Libc.free(s.metadata)
    if s.n_children > 0 && s.children != C_NULL
        children_ptr = Ptr{Ptr{ArrowSchema}}(s.children)
        for i in 1:s.n_children
            child = unsafe_load(children_ptr, i)
            if child != C_NULL
                _free_schema_recursive(child)
                Libc.free(child)
            end
        end
        Libc.free(s.children)
    end
end

"""
    release_arrow_schema(schema)

Free all memory allocated by `make_schema` / `make_vector_schema`.
Must be called exactly once after the schema is no longer needed.
"""
function release_arrow_schema(schema::Ptr{ArrowSchema})
    schema == C_NULL && return
    _free_schema_recursive(schema)
    Libc.free(schema)
end
