# ── Query ─────────────────────────────────────────────────────────────────────

"""
    Query

Builder for full-table scans. Chainable methods mutate the query in place
(unlike the C builder pattern, which would consume and return a new pointer).

    result = query(tbl) |> filter_where("year > 2020") |> limit(100) |> execute
"""
mutable struct Query
    handle::Ptr{LanceDBQueryHandle}
    _consumed::Bool

    function Query(tbl::Table)
        handle = lancedb_query_new(tbl.handle)
        check_ptr(handle, "lancedb_query_new returned NULL")
        q = new(handle, false)
        finalizer(q -> q._consumed || lancedb_query_free(q.handle), q)
        q
    end
end

function _assert_live(q::Query)
    q._consumed && throw(LanceDBException(Int32(LANCEDB_RUNTIME), "Query already executed"))
end

query(tbl::Table) = Query(tbl)

function limit(q::Query, n::Integer)::Query
    _assert_live(q)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_query_limit(q.handle, Csize_t(n), errmsg), errmsg)
    q
end

limit(n::Integer) = q -> limit(q, n)

function offset(q::Query, n::Integer)::Query
    _assert_live(q)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_query_offset(q.handle, Csize_t(n), errmsg), errmsg)
    q
end

offset(n::Integer) = q -> offset(q, n)

function select_cols(q::Query, cols::Vector{String})::Query
    _assert_live(q)
    ptrs   = [pointer(c) for c in cols]
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    GC.@preserve cols begin
        code = lancedb_query_select(q.handle, pointer(ptrs), Csize_t(length(cols)), errmsg)
    end
    check(code, errmsg)
    q
end

select_cols(cols::Vector{String}) = q -> select_cols(q, cols)

"""
    filter_where(q, predicate) -> Query

Set a SQL WHERE predicate. If a DataFusion expression is also set it takes
precedence (see `filter_expr`).
"""
function filter_where(q::Query, predicate::AbstractString)::Query
    _assert_live(q)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_query_where_filter(q.handle, predicate, errmsg), errmsg)
    q
end

filter_where(predicate::AbstractString) = q -> filter_where(q, predicate)

"""
    filter_expr(q, expr) -> Query

Set a DataFusion `LanceDBExpr` filter. The expr handle is consumed.
"""
function filter_expr(q::Query, expr::LanceDBExpr)::Query
    _assert_live(q)
    expr._consumed && throw(LanceDBException(Int32(LANCEDB_RUNTIME), "Expr already consumed"))
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_query_df_filter(q.handle, expr.handle, errmsg), errmsg)
    expr._consumed = true
    q
end

"""
    execute(q) -> QueryResult

Run the query. Consumes the query handle.
"""
function execute(q::Query)::QueryResult
    _assert_live(q)
    result = lancedb_query_execute(q.handle)
    q._consumed = true
    check_ptr(result, "lancedb_query_execute returned NULL")
    qr = QueryResult(result)
    finalizer(r -> r.handle != C_NULL && lancedb_query_result_free(r.handle), qr)
    qr
end

# ── VectorQuery ───────────────────────────────────────────────────────────────

"""
    VectorQuery

Builder for ANN (approximate nearest-neighbour) search.

    result = vector_search(tbl, query_vec, "embedding") |>
             distance_type(Cosine) |>
             nprobes(20) |>
             limit(10) |>
             execute
"""
mutable struct VectorQuery
    handle::Ptr{LanceDBVectorQueryHandle}
    _consumed::Bool

    function VectorQuery(tbl::Table, vec::Vector{Float32}, column::Union{String,Nothing}=nothing)
        GC.@preserve vec begin
            handle = lancedb_vector_query_new(tbl.handle, pointer(vec), Csize_t(length(vec)))
        end
        check_ptr(handle, "lancedb_vector_query_new returned NULL")
        vq = new(handle, false)
        if !isnothing(column)
            errmsg = Ref{Ptr{UInt8}}(C_NULL)
            check(lancedb_vector_query_column(vq.handle, column, errmsg), errmsg)
        end
        finalizer(vq -> vq._consumed || lancedb_vector_query_free(vq.handle), vq)
        vq
    end
end

function _assert_live(vq::VectorQuery)
    vq._consumed && throw(LanceDBException(Int32(LANCEDB_RUNTIME), "VectorQuery already executed"))
end

vector_search(tbl::Table, vec::Vector{Float32}, col::String)  = VectorQuery(tbl, vec, col)
vector_search(tbl::Table, vec::Vector{Float32})               = VectorQuery(tbl, vec)

function limit(vq::VectorQuery, n::Integer)::VectorQuery
    _assert_live(vq)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_vector_query_limit(vq.handle, Csize_t(n), errmsg), errmsg)
    vq
end

function distance_type(vq::VectorQuery, dt::DistanceType)::VectorQuery
    _assert_live(vq)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_vector_query_distance_type(vq.handle, Cint(dt), errmsg), errmsg)
    vq
end

distance_type(dt::DistanceType) = vq -> distance_type(vq, dt)

function nprobes(vq::VectorQuery, n::Integer)::VectorQuery
    _assert_live(vq)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_vector_query_nprobes(vq.handle, Csize_t(n), errmsg), errmsg)
    vq
end

nprobes(n::Integer) = vq -> nprobes(vq, n)

function refine_factor(vq::VectorQuery, k::Integer)::VectorQuery
    _assert_live(vq)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_vector_query_refine_factor(vq.handle, Cuint(k), errmsg), errmsg)
    vq
end

refine_factor(k::Integer) = vq -> refine_factor(vq, k)

function ef(vq::VectorQuery, n::Integer)::VectorQuery
    _assert_live(vq)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_vector_query_ef(vq.handle, Csize_t(n), errmsg), errmsg)
    vq
end

ef(n::Integer) = vq -> ef(vq, n)

function filter_where(vq::VectorQuery, predicate::AbstractString)::VectorQuery
    _assert_live(vq)
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    check(lancedb_vector_query_where_filter(vq.handle, predicate, errmsg), errmsg)
    vq
end

function select_cols(vq::VectorQuery, cols::Vector{String})::VectorQuery
    _assert_live(vq)
    ptrs   = [pointer(c) for c in cols]
    errmsg = Ref{Ptr{UInt8}}(C_NULL)
    GC.@preserve cols begin
        code = lancedb_vector_query_select(vq.handle, pointer(ptrs), Csize_t(length(cols)), errmsg)
    end
    check(code, errmsg)
    vq
end

function execute(vq::VectorQuery)::QueryResult
    _assert_live(vq)
    result = lancedb_vector_query_execute(vq.handle)
    vq._consumed = true
    check_ptr(result, "lancedb_vector_query_execute returned NULL")
    qr = QueryResult(result)
    finalizer(r -> r.handle != C_NULL && lancedb_query_result_free(r.handle), qr)
    qr
end
