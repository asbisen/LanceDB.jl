"""
    LanceDBExpr

Wraps a DataFusion expression handle. Expressions are consumed (transferred to
the C side) when passed to binary operators or query filters, so they cannot
be reused after that point.
"""
mutable struct LanceDBExpr
    handle::Ptr{LanceDBExprHandle}
    _consumed::Bool

    function LanceDBExpr(handle::Ptr{LanceDBExprHandle})
        check_ptr(handle, "expression constructor received NULL handle")
        e = new(handle, false)
        finalizer(e -> e._consumed || lancedb_expr_free(e.handle), e)
        e
    end
end

function _assert_live(e::LanceDBExpr)
    e._consumed && throw(LanceDBException(Int32(LANCEDB_RUNTIME), "LanceDBExpr already consumed"))
end

function _consume(e::LanceDBExpr)::Ptr{LanceDBExprHandle}
    _assert_live(e)
    e._consumed = true
    e.handle
end

# ── Constructors ──────────────────────────────────────────────────────────────

col(name::AbstractString)::LanceDBExpr    = LanceDBExpr(lancedb_expr_column(name))
lit(v::AbstractString)::LanceDBExpr       = LanceDBExpr(lancedb_expr_literal_string(v))
lit(v::Integer)::LanceDBExpr              = LanceDBExpr(lancedb_expr_literal_i64(Int64(v)))
lit(v::AbstractFloat)::LanceDBExpr        = LanceDBExpr(lancedb_expr_literal_f64(Float64(v)))
lit(v::Bool)::LanceDBExpr                 = LanceDBExpr(lancedb_expr_literal_bool(v))

# Clone an expression so it can be reused in multiple positions
Base.copy(e::LanceDBExpr)::LanceDBExpr    = LanceDBExpr(lancedb_expr_clone(e.handle))

# ── Unary operators ───────────────────────────────────────────────────────────

Base.:!(e::LanceDBExpr)::LanceDBExpr      = LanceDBExpr(lancedb_expr_not(_consume(e)))
isnull(e::LanceDBExpr)::LanceDBExpr       = LanceDBExpr(lancedb_expr_is_null(_consume(e)))
isnotnull(e::LanceDBExpr)::LanceDBExpr    = LanceDBExpr(lancedb_expr_is_not_null(_consume(e)))

# ── Binary operators ──────────────────────────────────────────────────────────

function _binary(a::LanceDBExpr, op::BinaryOp, b::LanceDBExpr)::LanceDBExpr
    LanceDBExpr(lancedb_expr_binary(_consume(a), Cint(op), _consume(b)))
end

Base.:(==)(a::LanceDBExpr, b::LanceDBExpr) = _binary(a, OpEq, b)
Base.:!=(a::LanceDBExpr, b::LanceDBExpr)   = _binary(a, OpNotEq, b)
Base.:<(a::LanceDBExpr, b::LanceDBExpr)    = _binary(a, OpLt, b)
Base.:<=(a::LanceDBExpr, b::LanceDBExpr)   = _binary(a, OpLtEq, b)
Base.:>(a::LanceDBExpr, b::LanceDBExpr)    = _binary(a, OpGt, b)
Base.:>=(a::LanceDBExpr, b::LanceDBExpr)   = _binary(a, OpGtEq, b)
Base.:&(a::LanceDBExpr, b::LanceDBExpr)    = LanceDBExpr(lancedb_expr_and(_consume(a), _consume(b)))
Base.:|(a::LanceDBExpr, b::LanceDBExpr)    = LanceDBExpr(lancedb_expr_or(_consume(a), _consume(b)))
Base.:+(a::LanceDBExpr, b::LanceDBExpr)    = _binary(a, OpPlus, b)
Base.:-(a::LanceDBExpr, b::LanceDBExpr)    = _binary(a, OpMinus, b)
Base.:*(a::LanceDBExpr, b::LanceDBExpr)    = _binary(a, OpMultiply, b)
Base.:/(a::LanceDBExpr, b::LanceDBExpr)    = _binary(a, OpDivide, b)
Base.:%(a::LanceDBExpr, b::LanceDBExpr)    = _binary(a, OpModulo, b)
