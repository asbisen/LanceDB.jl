# LanceDB.jl Quickstart

This guide walks through the full feature set from a Julia REPL. Every block
can be copy-pasted in sequence — each one builds on the previous.

## Prerequisites

### 1. Build the C library

```bash
cd lancedb-c/build
cmake ..
make
# Produces: lancedb-c/build/target/release/liblancedb.so
```

### 2. Start Julia in the package directory

```bash
cd LanceDB.jl
julia --project=.
```

```julia
using LanceDB
using Tables          # standard table interface — column access
```

---

## 1. Connect

```julia
conn = Connection("/tmp/my_lancedb")

println(uri(conn))          # "/tmp/my_lancedb"
println(table_names(conn))  # String[]  (empty database)
```

---

## 2. Create a table from data

Pass any named tuple, DataFrame, or Tables.jl-compatible object to
`create_table`. The schema is inferred from the column types.

```julia
movies = (
    id        = Int32[1, 2, 3, 4, 5],
    title     = ["Dune", "Arrival", "Interstellar", "Ex Machina", "Annihilation"],
    year      = Int32[2021, 2016, 2014, 2014, 2018],
    rating    = Float32[7.9, 7.9, 8.6, 7.7, 6.8],
    embedding = [
        Float32[0.9, 0.1, 0.2, 0.0],
        Float32[0.1, 0.8, 0.3, 0.2],
        Float32[0.8, 0.2, 0.5, 0.1],
        Float32[0.2, 0.7, 0.1, 0.5],
        Float32[0.3, 0.6, 0.4, 0.3],
    ],
)

tbl = create_table(conn, "movies", movies)
println(count_rows(tbl))  # 5
```

Supported column element types: `Int8/16/32/64`, `UInt8/16/32/64`,
`Float32/64`, `AbstractString` (UTF-8), `AbstractVector{Float32}`
(fixed-size embedding vectors).

### Create an empty table with an explicit schema

```julia
schema = make_vector_schema("id", "embedding", 4)   # utf8 + FixedSizeList[4]
empty_tbl = create_table(conn, "scratch", schema)
release_arrow_schema(schema)                         # free the schema pointer
```

For arbitrary schemas use `make_schema`:

```julia
schema = make_schema(["name" => "u", "score" => "f", "count" => "l"])
# format strings: "u"=UTF-8, "f"=Float32, "g"=Float64,
#                 "i"=Int32, "l"=Int64, "+w:N"=FixedSizeList[N]
release_arrow_schema(schema)
```

---

## 3. Add rows

`add` accepts the same types as `create_table`:

```julia
add(tbl, (
    id        = Int32[6, 7],
    title     = ["Blade Runner 2049", "Her"],
    year      = Int32[2017, 2013],
    rating    = Float32[8.0, 8.0],
    embedding = [Float32[0.7, 0.3, 0.4, 0.1],
                 Float32[0.4, 0.5, 0.6, 0.2]],
))

println(count_rows(tbl))   # 7
println(table_version(tbl)) # version increments on every write
```

---

## 4. Full-table scan

```julia
result = query(tbl) |> execute
cols   = Tables.columns(result)

println(sort(collect(cols[:title])))
# ["Annihilation", "Arrival", "Blade Runner 2049", "Dune",
#  "Ex Machina", "Her", "Interstellar"]
```

`QueryResult` implements `Tables.jl`, so it works with DataFrames.jl,
CSV.jl, and any other ecosystem package:

```julia
# using DataFrames
# df = DataFrame(Tables.columns(result))
```

---

## 5. SQL string filters

`filter_where` accepts a SQL WHERE predicate:

```julia
cols = Tables.columns(
    query(tbl) |> filter_where("rating > 7.8") |> execute
)
println(sort(collect(cols[:title])))
# ["Arrival", "Blade Runner 2049", "Dune", "Her", "Interstellar"]

cols = Tables.columns(
    query(tbl) |> filter_where("year >= 2016 AND year <= 2021") |> execute
)
println(sort(collect(cols[:title])))
# ["Arrival", "Blade Runner 2049", "Dune"]
```

---

## 6. Expression DSL filters

Build filters programmatically with `col`, `lit`, and Julia operators.

```julia
# Single comparison
cols = Tables.columns(
    query(tbl) |> filter_expr(col("year") > lit(2016)) |> execute
)
println(sort(collect(cols[:title])))
# ["Blade Runner 2049", "Dune"]

# AND — wrap each side in parentheses (& binds tighter than comparisons in Julia)
e = (col("rating") >= lit(7.9f0)) & (col("year") <= lit(2018))
cols = Tables.columns(query(tbl) |> filter_expr(e) |> execute)
println(sort(collect(cols[:title])))
# ["Annihilation", "Arrival", "Ex Machina"]

# OR
e = (col("rating") > lit(8.5f0)) | (col("year") < lit(2014))
cols = Tables.columns(query(tbl) |> filter_expr(e) |> execute)
println(sort(collect(cols[:title])))
# ["Her", "Interstellar"]

# NOT
e = !(col("title") == lit("Dune"))
cols = Tables.columns(query(tbl) |> filter_expr(e) |> execute)
println(length(cols[:id]))  # 6

# IN list
e = isin(col("id"), lit(1), lit(3), lit(5))
cols = Tables.columns(query(tbl) |> filter_expr(e) |> execute)
println(sort(collect(cols[:title])))
# ["Annihilation", "Dune", "Interstellar"]

# NOT IN
e = notiin(col("title"), lit("Dune"), lit("Arrival"))
cols = Tables.columns(query(tbl) |> filter_expr(e) |> execute)
println(length(cols[:id]))  # 5

# IS NULL / IS NOT NULL
e = isnotnull(col("title"))
cols = Tables.columns(query(tbl) |> filter_expr(e) |> execute)
println(count_rows(tbl) == length(cols[:id]))  # true

# Arithmetic
e = (col("rating") * lit(10.0f0)) > lit(85.0f0)
cols = Tables.columns(query(tbl) |> filter_expr(e) |> execute)
println(sort(collect(cols[:title])))
# ["Blade Runner 2049", "Her", "Interstellar"]
```

Copy an expression to reuse it:

```julia
base = col("year") > lit(2015)
e1   = copy(base) & (col("rating") > lit(7.8f0))
e2   = copy(base) & (col("rating") < lit(7.5f0))
# base is still valid after copy()
```

---

## 7. Column projection, limit, and offset

```julia
# Select specific columns
cols = Tables.columns(
    query(tbl) |> select_cols(["id", "title", "rating"]) |> execute
)
println(haskey(cols, :year))      # false
println(haskey(cols, :embedding)) # false

# Limit number of results
cols = Tables.columns(query(tbl) |> limit(3) |> execute)
println(length(cols[:id]))  # 3

# Paginate with offset
page1 = Tables.columns(query(tbl) |> limit(3) |> offset(0) |> execute)
page2 = Tables.columns(query(tbl) |> limit(3) |> offset(3) |> execute)
page3 = Tables.columns(query(tbl) |> limit(3) |> offset(6) |> execute)
println(length(page1[:id]), " ", length(page2[:id]), " ", length(page3[:id]))
# 3 3 1
```

Chain any combination — order doesn't matter:

```julia
cols = Tables.columns(
    query(tbl) |>
    filter_where("rating > 7.5") |>
    select_cols(["title", "rating"]) |>
    limit(5) |>
    offset(1) |>
    execute
)
```

---

## 8. Vector search (ANN)

```julia
query_vec = Float32[0.8, 0.2, 0.5, 0.1]   # similar to "Interstellar"

cols = Tables.columns(
    vector_search(tbl, query_vec, "embedding") |> limit(3) |> execute
)
println(collect(cols[:title]))     # nearest first
println(collect(cols[:_distance])) # L2 distances, ascending
```

### Distance types

```julia
# Cosine similarity (good for normalized embeddings)
cols = Tables.columns(
    vector_search(tbl, query_vec, "embedding") |>
    distance_type(Cosine) |>
    limit(3) |>
    execute
)

# Other options: Dot, Hamming (for binary vectors)
```

### Combine with filters and column projection

```julia
cols = Tables.columns(
    vector_search(tbl, query_vec, "embedding") |>
    filter_where("year > 2015") |>
    select_cols(["title", "year", "_distance"]) |>
    limit(5) |>
    execute
)

# DSL filter on vector search
cols = Tables.columns(
    vector_search(tbl, query_vec, "embedding") |>
    filter_expr(col("rating") > lit(7.5f0)) |>
    limit(5) |>
    execute
)
```

---

## 9. Indexes

### Vector index (IVFFlat)

A vector index speeds up ANN search at scale. It requires enough rows to
train — at least 256 for the default configuration.

```julia
# Build a larger table for indexing
n   = 300
big = (
    id        = string.(1:n),
    embedding = [Float32.(rand(4)) for _ in 1:n],
)
big_tbl = create_table(conn, "big", big)

cfg = LanceDBVectorIndexConfig()
cfg.num_partitions = 8   # tune to √n for production tables

create_vector_index(big_tbl, "embedding"; type=IVFFlat, config=cfg)
println(list_indices(big_tbl))  # ["embedding_idx"]

stats = index_stats(big_tbl, "embedding_idx")
println("indexed: ", stats.num_indexed_rows)    # 300
println("unindexed: ", stats.num_unindexed_rows) # 0

# After adding new rows, they sit in the unindexed delta
add(big_tbl, (id=["x","y"], embedding=[Float32.(rand(4)), Float32.(rand(4))]))
println(index_stats(big_tbl, "embedding_idx").num_unindexed_rows) # 2

# Re-index the delta
optimize(big_tbl; type=OptimizeIndex)
println(index_stats(big_tbl, "embedding_idx").num_unindexed_rows) # 0

# Use nprobes to trade recall for speed (higher = better recall)
cols = Tables.columns(
    vector_search(big_tbl, Float32.(rand(4)), "embedding") |>
    nprobes(8) |>
    limit(10) |>
    execute
)
println(length(cols[:id]))  # 10

close(big_tbl)
```

### Scalar index (BTree)

```julia
create_scalar_index(tbl, "year")
println(list_indices(tbl))  # ["year_idx"]

stats = index_stats(tbl, "year_idx")
println("indexed: ", stats.num_indexed_rows)
```

### Full-text search index

```julia
create_fts_index(tbl, "title")
println(list_indices(tbl))  # ["year_idx", "title_idx"]

# Drop an index
drop_index(tbl, "title_idx")
```

### Compact and prune old versions

```julia
optimize(tbl)                            # runs all optimizations
optimize(tbl; type=OptimizeCompact)      # compact small files
optimize(tbl; type=OptimizePrune)        # remove old version files
optimize(tbl; type=OptimizeIndex)        # re-index delta rows
```

---

## 10. Delete rows

```julia
println(count_rows(tbl))   # 7
v_before = table_version(tbl)

delete_rows(tbl, "rating < 7.5")  # SQL predicate

println(count_rows(tbl))           # 5 (Annihilation removed)
println(table_version(tbl) > v_before)  # true — version bumped
```

---

## 11. Upsert (merge_insert)

`merge_insert` updates rows that match on a key column and inserts rows that
don't exist yet:

```julia
updates = (
    id        = Int32[3, 8],
    title     = ["Interstellar (Director's Cut)", "Dune: Part Two"],
    year      = Int32[2014, 2024],
    rating    = Float32[9.0, 8.5],
    embedding = [Float32[0.8, 0.2, 0.5, 0.1], Float32[0.95, 0.05, 0.15, 0.0]],
)

merge_insert(tbl, updates, "id")
# id=3 updated (new title and rating), id=8 inserted

cols = Tables.columns(query(tbl) |> filter_where("id = 3") |> execute)
println(cols[:title][1])   # "Interstellar (Director's Cut)"
println(cols[:rating][1])  # 9.0

# Multi-column key
merge_insert(tbl, updates, ["id", "year"])
```

---

## 12. Persistence

Tables are stored on disk. Close a connection and everything survives:

```julia
close(tbl)
close(conn)

# --- new Julia session or later in the same session ---

conn2 = Connection("/tmp/my_lancedb")
println(table_names(conn2))  # ["movies", "big", "scratch"]

tbl2 = open_table(conn2, "movies")
println(count_rows(tbl2))    # rows survive across sessions

# Drop a table permanently
drop_table(conn2, "scratch")
println(table_names(conn2))  # scratch is gone

close(tbl2)
close(conn2)
```

---

## 13. Deterministic cleanup with do-blocks

```julia
open(Connection, "/tmp/my_lancedb") do conn
    tbl = open_table(conn, "movies")
    result = query(tbl) |> limit(3) |> execute
    println(length(Tables.columns(result)[:id]))  # 3
    close(tbl)
end  # conn.close() called automatically
```

---

## Quick reference

| Operation | Example |
|---|---|
| Connect | `conn = Connection("/path/to/db")` |
| Create from data | `tbl = create_table(conn, "name", data)` |
| Create empty | `tbl = create_table(conn, "name", make_vector_schema("id","vec",128))` |
| Add rows | `add(tbl, data)` |
| Full scan | `query(tbl) \|> execute` |
| SQL filter | `query(tbl) \|> filter_where("col > 5") \|> execute` |
| DSL filter | `query(tbl) \|> filter_expr(col("x") > lit(5)) \|> execute` |
| Vector search | `vector_search(tbl, vec, "col") \|> limit(10) \|> execute` |
| Limit / offset | `query(tbl) \|> limit(10) \|> offset(20) \|> execute` |
| Column select | `query(tbl) \|> select_cols(["a","b"]) \|> execute` |
| Delete | `delete_rows(tbl, "id > 100")` |
| Upsert | `merge_insert(tbl, data, "id")` |
| Row count | `count_rows(tbl)` |
| Version | `table_version(tbl)` |
| List tables | `table_names(conn)` |
| Open existing | `tbl = open_table(conn, "name")` |
| Drop table | `drop_table(conn, "name")` |
| Vector index | `create_vector_index(tbl, "vec"; type=IVFFlat)` |
| Scalar index | `create_scalar_index(tbl, "col")` |
| FTS index | `create_fts_index(tbl, "text_col")` |
| List indices | `list_indices(tbl)` |
| Drop index | `drop_index(tbl, "col_idx")` |
| Index stats | `index_stats(tbl, "col_idx")` |
| Optimize | `optimize(tbl)` |

### Expression DSL operators

| Julia | SQL equivalent |
|---|---|
| `col("x") == lit(v)` | `x = v` |
| `col("x") != lit(v)` | `x != v` |
| `col("x") > lit(v)` | `x > v` |
| `col("x") >= lit(v)` | `x >= v` |
| `(e1) & (e2)` | `e1 AND e2` |
| `(e1) \| (e2)` | `e1 OR e2` |
| `!(e)` | `NOT e` |
| `isin(col("x"), lit(a), lit(b))` | `x IN (a, b)` |
| `notiin(col("x"), lit(a), lit(b))` | `x NOT IN (a, b)` |
| `isnull(col("x"))` | `x IS NULL` |
| `isnotnull(col("x"))` | `x IS NOT NULL` |
| `col("x") * lit(2.0f0)` | `x * 2.0` |

> **Precedence note:** In Julia, `&` and `|` bind *tighter* than comparison
> operators. Always wrap each comparison in parentheses:
> `(col("a") > lit(1)) & (col("b") < lit(10))`.
