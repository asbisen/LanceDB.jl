# Opaque C handle types — match the typedefs in lancedb.h.
# Each primitive type is a distinct 64-bit word, giving the Julia type system
# visibility into what a pointer refers to without exposing arithmetic.

primitive type LanceDBConnectBuilderHandle 64 end
primitive type LanceDBConnectionHandle     64 end
primitive type LanceDBTableHandle          64 end
primitive type LanceDBTableNamesBuilderHandle 64 end
primitive type LanceDBQueryHandle          64 end
primitive type LanceDBVectorQueryHandle    64 end
primitive type LanceDBQueryResultHandle    64 end
primitive type LanceDBExprHandle           64 end
primitive type LanceDBSessionHandle        64 end
primitive type LanceDBRecordBatchReaderHandle 64 end

# ── Enums ─────────────────────────────────────────────────────────────────────

@enum LanceDBError::Int32 begin
    LANCEDB_SUCCESS                    = 0
    LANCEDB_INVALID_ARGUMENT           = 1
    LANCEDB_INVALID_TABLE_NAME         = 2
    LANCEDB_INVALID_INPUT              = 3
    LANCEDB_TABLE_NOT_FOUND            = 4
    LANCEDB_DATABASE_NOT_FOUND         = 5
    LANCEDB_DATABASE_ALREADY_EXISTS    = 6
    LANCEDB_INDEX_NOT_FOUND            = 7
    LANCEDB_EMBEDDING_FUNCTION_NOT_FOUND = 8
    LANCEDB_TABLE_ALREADY_EXISTS       = 9
    LANCEDB_CREATE_DIR                 = 10
    LANCEDB_SCHEMA                     = 11
    LANCEDB_RUNTIME                    = 12
    LANCEDB_TIMEOUT                    = 13
    LANCEDB_OBJECT_STORE               = 14
    LANCEDB_LANCE                      = 15
    LANCEDB_HTTP                       = 16
    LANCEDB_RETRY                      = 17
    LANCEDB_ARROW                      = 18
    LANCEDB_NOT_SUPPORTED              = 19
    LANCEDB_OTHER                      = 20
    LANCEDB_UNKNOWN                    = 21
end

@enum DistanceType::Int32 begin
    L2      = 0
    Cosine  = 1
    Dot     = 2
    Hamming = 3
end

@enum IndexType::Int32 begin
    Auto      = 0
    BTree     = 1
    Bitmap    = 2
    LabelList = 3
    FTS       = 4
    IVFFlat   = 5
    IVFPQ     = 6
    IVFHNSWpq = 7
    IVFHNSWsq = 8
end

@enum OptimizeType::Int32 begin
    OptimizeAll     = 0
    OptimizeCompact = 1
    OptimizePrune   = 2
    OptimizeIndex   = 3
end

@enum BinaryOp::Int32 begin
    OpEq       = 0
    OpNotEq    = 1
    OpLt       = 2
    OpLtEq     = 3
    OpGt       = 4
    OpGtEq     = 5
    OpAnd      = 6
    OpOr       = 7
    OpPlus     = 8
    OpMinus    = 9
    OpMultiply = 10
    OpDivide   = 11
    OpModulo   = 12
end

# ── C value-type structs ───────────────────────────────────────────────────────

mutable struct LanceDBVectorIndexConfig
    num_partitions::Cint
    num_sub_vectors::Cint
    max_iterations::Cint
    sample_rate::Cfloat
    distance_type::Cint   # DistanceType
    accelerator::Ptr{UInt8}
    replace::Cint
end

LanceDBVectorIndexConfig() = LanceDBVectorIndexConfig(-1, -1, -1, 0.0f0, Int32(L2), C_NULL, 0)

mutable struct LanceDBScalarIndexConfig
    replace::Cint
    force_update_statistics::Cint
end

LanceDBScalarIndexConfig() = LanceDBScalarIndexConfig(0, 0)

mutable struct LanceDBFtsIndexConfig
    base_tokenizer::Ptr{UInt8}
    language::Ptr{UInt8}
    max_tokens::Cint
    lowercase::Cint
    stem::Cint
    remove_stop_words::Cint
    ascii_folding::Cint
    replace::Cint
end

LanceDBFtsIndexConfig() = LanceDBFtsIndexConfig(C_NULL, C_NULL, -1, 1, 0, 0, 0, 0)

mutable struct LanceDBMergeInsertConfig
    when_matched_update_all::Cint
    when_not_matched_insert_all::Cint
end

LanceDBMergeInsertConfig() = LanceDBMergeInsertConfig(1, 1)

mutable struct LanceDBSessionOptions
    index_cache_bytes::Csize_t
    metadata_cache_bytes::Csize_t
end

LanceDBSessionOptions() = LanceDBSessionOptions(0, 0)

mutable struct LanceDBSessionCacheStats
    hits::UInt64
    misses::UInt64
    num_entries::Csize_t
    size_bytes::Csize_t
end

mutable struct LanceDBVersion
    version::UInt64
    timestamp_seconds::Int64
    timestamp_nanos::UInt32
end

mutable struct LanceDBVersionMetadata
    keys::Ptr{Ptr{UInt8}}
    values::Ptr{Ptr{UInt8}}
    count::Csize_t
end

mutable struct LanceDBIndexStats
    num_indexed_rows::Csize_t
    num_unindexed_rows::Csize_t
    num_indices::Cuint
end
