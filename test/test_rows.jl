@testset "QueryResult row access" begin
    tmp = mktempdir()

    # ── 1. rowaccess is declared ──────────────────────────────────────────────
    @testset "rowaccess declared" begin
        conn = Connection(joinpath(tmp, "rows-decl"))
        tbl  = create_table(conn, "t", (x = Int32[1],))
        qr   = query(tbl) |> execute
        @test Tables.rowaccess(qr) === true
        close(tbl); close(conn)
    end

    # ── 2. rows returns correct length ───────────────────────────────────────
    @testset "rows length matches row count" begin
        conn = Connection(joinpath(tmp, "rows-len"))
        tbl  = create_table(conn, "t", (id = Int32[1, 2, 3, 4, 5],))
        qr   = query(tbl) |> execute
        @test length(Tables.rows(qr)) == 5
        close(tbl); close(conn)
    end

    # ── 3. iterate over rows, check column values ─────────────────────────────
    @testset "iterate rows, column values correct" begin
        conn = Connection(joinpath(tmp, "rows-iter"))
        data = (id = Int64[10, 20, 30], label = ["a", "b", "c"])
        tbl  = create_table(conn, "t", data)
        qr   = query(tbl) |> execute

        rows = collect(Tables.rows(qr))
        @test length(rows) == 3

        id_vals    = sort([Tables.getcolumn(r, :id)    for r in rows])
        label_vals = sort([Tables.getcolumn(r, :label) for r in rows])
        @test id_vals    == Int64[10, 20, 30]
        @test label_vals == ["a", "b", "c"]

        close(tbl); close(conn)
    end

    # ── 4. columnnames on a row ───────────────────────────────────────────────
    @testset "columnnames on row" begin
        conn = Connection(joinpath(tmp, "rows-names"))
        data = (x = Float32[1.0f0], y = Float32[2.0f0], z = Float32[3.0f0])
        tbl  = create_table(conn, "t", data)
        qr   = query(tbl) |> execute

        row = first(Tables.rows(qr))
        @test sort(collect(Tables.columnnames(row))) == [:x, :y, :z]

        close(tbl); close(conn)
    end

    # ── 5. getcolumn by positional index ─────────────────────────────────────
    @testset "getcolumn by index" begin
        conn = Connection(joinpath(tmp, "rows-idx"))
        data = (a = Int32[42], b = Float32[1.5f0])
        tbl  = create_table(conn, "t", data)
        qr   = query(tbl) |> execute

        row = first(Tables.rows(qr))
        # Column order is preserved; verify both indices return the right type
        @test Tables.getcolumn(row, 1) isa Int32
        @test Tables.getcolumn(row, 2) isa Float32

        close(tbl); close(conn)
    end

    # ── 6. zero-row result produces empty iterator ────────────────────────────
    @testset "empty table gives zero rows" begin
        conn   = Connection(joinpath(tmp, "rows-empty"))
        schema = make_schema(["val" => "i"])
        tbl    = create_table(conn, "t", schema)
        release_arrow_schema(schema)

        qr = query(tbl) |> execute
        @test length(Tables.rows(qr)) == 0
        @test isempty(collect(Tables.rows(qr)))

        close(tbl); close(conn)
    end

    # ── 7. filtered result row count matches ─────────────────────────────────
    @testset "rows after filter" begin
        conn = Connection(joinpath(tmp, "rows-filter"))
        data = (id = Int32[1, 2, 3, 4, 5], v = Float32[1, 2, 3, 4, 5])
        tbl  = create_table(conn, "t", data)
        qr   = query(tbl) |> filter_where("v > 3") |> execute

        rows = collect(Tables.rows(qr))
        @test length(rows) == 2
        @test all(r -> Tables.getcolumn(r, :v) > 3.0f0, rows)

        close(tbl); close(conn)
    end

    # ── 8. row access and column access agree on values ───────────────────────
    @testset "rows and columns agree" begin
        conn = Connection(joinpath(tmp, "rows-agree"))
        data = (k = Int64[5, 10, 15], w = Float64[0.5, 1.0, 1.5])
        tbl  = create_table(conn, "t", data)
        qr   = query(tbl) |> execute

        cols     = Tables.columns(qr)
        rows     = collect(Tables.rows(qr))
        k_via_rows = sort([Tables.getcolumn(r, :k) for r in rows])
        @test k_via_rows == sort(collect(cols[:k]))

        close(tbl); close(conn)
    end

    # ── 9. vector column readable row-by-row ─────────────────────────────────
    @testset "vector column readable per row" begin
        conn = Connection(joinpath(tmp, "rows-vec"))
        vecs = [Float32[1, 0, 0], Float32[0, 1, 0], Float32[0, 0, 1]]
        data = (id = ["x", "y", "z"], vec = vecs)
        tbl  = create_table(conn, "t", data)
        qr   = query(tbl) |> execute

        rows = collect(Tables.rows(qr))
        @test length(rows) == 3
        got = sort([Tables.getcolumn(r, :vec) for r in rows])
        @test all(v -> length(v) == 3, got)

        close(tbl); close(conn)
    end

    # ── 10. rows is idempotent (materialise not repeated) ─────────────────────
    @testset "rows called twice reuses cache" begin
        conn = Connection(joinpath(tmp, "rows-idem"))
        tbl  = create_table(conn, "t", (n = Int32[7, 8, 9],))
        qr   = query(tbl) |> execute

        r1 = collect(Tables.rows(qr))
        r2 = collect(Tables.rows(qr))
        @test length(r1) == length(r2) == 3
        @test [Tables.getcolumn(r, :n) for r in r1] ==
              [Tables.getcolumn(r, :n) for r in r2]

        close(tbl); close(conn)
    end
end
