"""
    create_vector_index(tbl, columns; type=Auto, config=LanceDBVectorIndexConfig())

Create a vector index on the given columns.
"""
function create_vector_index(tbl::Table, columns::Vector{String};
                              type::IndexType=Auto,
                              config::LanceDBVectorIndexConfig=LanceDBVectorIndexConfig())
    ptrs   = [pointer(c) for c in columns]
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    GC.@preserve columns begin
        code = lancedb_table_create_vector_index(
            tbl.handle,
            pointer(ptrs), Csize_t(length(columns)),
            Cint(type), Ref(config), errmsg
        )
    end
    check(code, errmsg)
end

create_vector_index(tbl::Table, column::String; kwargs...) =
    create_vector_index(tbl, [column]; kwargs...)

"""
    create_scalar_index(tbl, columns; type=BTree, config=LanceDBScalarIndexConfig())
"""
function create_scalar_index(tbl::Table, columns::Vector{String};
                              type::IndexType=BTree,
                              config::LanceDBScalarIndexConfig=LanceDBScalarIndexConfig())
    ptrs   = [pointer(c) for c in columns]
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    GC.@preserve columns begin
        code = lancedb_table_create_scalar_index(
            tbl.handle,
            pointer(ptrs), Csize_t(length(columns)),
            Cint(type), Ref(config), errmsg
        )
    end
    check(code, errmsg)
end

create_scalar_index(tbl::Table, column::String; kwargs...) =
    create_scalar_index(tbl, [column]; kwargs...)

"""
    create_fts_index(tbl, columns; config=LanceDBFtsIndexConfig())
"""
function create_fts_index(tbl::Table, columns::Vector{String};
                           config::LanceDBFtsIndexConfig=LanceDBFtsIndexConfig())
    ptrs   = [pointer(c) for c in columns]
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    GC.@preserve columns begin
        code = lancedb_table_create_fts_index(
            tbl.handle,
            pointer(ptrs), Csize_t(length(columns)),
            Ref(config), errmsg
        )
    end
    check(code, errmsg)
end

create_fts_index(tbl::Table, column::String; kwargs...) =
    create_fts_index(tbl, [column]; kwargs...)

"""
    list_indices(tbl) -> Vector{String}
"""
function list_indices(tbl::Table)::Vector{String}
    indices_out = Ref{Ptr{Ptr{UInt8}}}(C_NULL)
    count_out   = Ref{Csize_t}(0)
    errmsg      = Ref{Ptr{UInt8}}(C_NULL)
    code = lancedb_table_list_indices(tbl.handle, indices_out, count_out, errmsg)
    check(code, errmsg)
    n   = count_out[]
    ptr = indices_out[]
    result = [unsafe_string(unsafe_load(ptr, i)) for i in 1:n]
    lancedb_free_index_list(ptr, n)
    result
end

"""
    drop_index(tbl, name)
"""
function drop_index(tbl::Table, name::AbstractString)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_table_drop_index(tbl.handle, name, errmsg), errmsg)
end

"""
    index_stats(tbl, name) -> LanceDBIndexStats
"""
function index_stats(tbl::Table, name::AbstractString)::LanceDBIndexStats
    stats  = Ref(LanceDBIndexStats(0, 0, 0))
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_table_index_stats(tbl.handle, name, stats, errmsg), errmsg)
    stats[]
end
