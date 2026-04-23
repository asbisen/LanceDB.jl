"""
    Table

An open LanceDB table handle. Freed automatically by the GC via finalizer;
call `close(tbl)` for deterministic cleanup.
"""
mutable struct Table
    handle::Ptr{LanceDBTableHandle}
    name::String
end

"""
    close(tbl::Table)

Release the native table handle immediately. Safe to call more than once.
After closing, any further operations on `tbl` will error.
"""
function Base.close(tbl::Table)
    tbl.handle == C_NULL && return
    lancedb_table_free(tbl.handle)
    tbl.handle = C_NULL
    nothing
end

Base.show(io::IO, tbl::Table) = print(io, "Table(\"$(tbl.name)\")")

"""
    count_rows(tbl) -> Int
"""
function count_rows(tbl::Table)::Int
    Int(lancedb_table_count_rows(tbl.handle))
end

"""
    table_version(tbl) -> Int
"""
function table_version(tbl::Table)::Int
    Int(lancedb_table_version(tbl.handle))
end

"""
    delete_rows(tbl, predicate)

Delete rows matching the SQL predicate string.
"""
function delete_rows(tbl::Table, predicate::AbstractString)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    code   = lancedb_table_delete(tbl.handle, predicate, errmsg)
    check(code, errmsg)
end

"""
    add(tbl, reader)

Append data using a raw `LanceDBRecordBatchReaderHandle` (consumed by this call).
"""
function add(tbl::Table, reader::Ptr{LanceDBRecordBatchReaderHandle})
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    code   = lancedb_table_add(tbl.handle, reader, errmsg)
    check(code, errmsg)
end

"""
    add(tbl, data)

Append rows from any Tables.jl-compatible source (NamedTuple, DataFrame, …).

Supported column element types: Int8/16/32/64, UInt8/16/32/64, Float32/64,
AbstractString (UTF-8), and AbstractVector{Float32} (FixedSizeList embeddings).
"""
function add(tbl::Table, data)
    Tables.istable(data) || throw(ArgumentError("data must satisfy the Tables.jl interface"))
    reader, schema_ptr, arr_hdr, pins = _make_reader(data)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    GC.@preserve pins begin
        code = lancedb_table_add(tbl.handle, reader, errmsg)
    end
    _free_array_tree(arr_hdr)
    release_arrow_schema(schema_ptr)
    check(code, errmsg)
    nothing
end

"""
    merge_insert(tbl, reader, on_columns; config=LanceDBMergeInsertConfig())

Upsert rows keyed on `on_columns`. The reader is consumed by this call.
"""
function merge_insert(tbl::Table,
                      reader::Ptr{LanceDBRecordBatchReaderHandle},
                      on_columns::Vector{String};
                      config::LanceDBMergeInsertConfig=LanceDBMergeInsertConfig())
    ptrs = [pointer(c) for c in on_columns]
    GC.@preserve on_columns begin
        col_ptrs = Ptr{Ptr{UInt8}}(pointer(ptrs))
        errmsg   = Ref{Ptr{UInt8}}(C_NULL)
        code     = lancedb_table_merge_insert(tbl.handle, reader, col_ptrs,
                                               Csize_t(length(on_columns)),
                                               Ref(config), errmsg)
        check(code, errmsg)
    end
end

"""
    merge_insert(tbl, data, on_columns; config=LanceDBMergeInsertConfig())

Upsert rows from any Tables.jl-compatible source, keyed on `on_columns`.
Matched rows are updated; unmatched rows are inserted.
"""
function merge_insert(tbl::Table, data, on_columns::Vector{String};
                      config::LanceDBMergeInsertConfig=LanceDBMergeInsertConfig())
    Tables.istable(data) || throw(ArgumentError("data must satisfy the Tables.jl interface"))
    reader, schema_ptr, arr_hdr, pins = _make_reader(data)
    ptrs = [pointer(c) for c in on_columns]
    GC.@preserve pins on_columns begin
        col_ptrs = Ptr{Ptr{UInt8}}(pointer(ptrs))
        errmsg   = Ref{Ptr{UInt8}}(C_NULL)
        code     = lancedb_table_merge_insert(tbl.handle, reader, col_ptrs,
                                               Csize_t(length(on_columns)),
                                               Ref(config), errmsg)
        _free_array_tree(arr_hdr)
        release_arrow_schema(schema_ptr)
        check(code, errmsg)
    end
    nothing
end

merge_insert(tbl::Table, data, on_column::String; kwargs...) =
    merge_insert(tbl, data, [on_column]; kwargs...)

"""
    optimize(tbl; type=OptimizeAll)

Compact files and/or prune old versions.
"""
function optimize(tbl::Table; type::OptimizeType=OptimizeAll)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    code   = lancedb_table_optimize(tbl.handle, Cint(type), errmsg)
    check(code, errmsg)
end
