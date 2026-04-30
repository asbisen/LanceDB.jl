@testset "Tables.jl sink protocol" begin
    tmp = mktempdir()

    # ── 1. materializer(tbl) appends rows and returns the same Table object ─────
    @testset "materializer(tbl) appends rows, returns tbl" begin
        conn = Connection(joinpath(tmp, "sink-add"))
        data = (id = Int32[1, 2, 3], label = ["a", "b", "c"])
        tbl  = create_table(conn, "t", data)
        @test count_rows(tbl) == 3

        more   = (id = Int32[4, 5], label = ["d", "e"])
        result = Tables.materializer(tbl)(more)
        @test result === tbl
        @test count_rows(tbl) == 5

        close(tbl); close(conn)
    end

    # ── 2. pipe syntax: data |> Tables.materializer(tbl) ─────────────────────────
    @testset "pipe syntax appends rows" begin
        conn = Connection(joinpath(tmp, "sink-pipe"))
        data = (x = Float32[1.0f0, 2.0f0], y = Float32[3.0f0, 4.0f0])
        tbl  = create_table(conn, "t", data)
        @test count_rows(tbl) == 2

        (x = Float32[5.0f0], y = Float32[6.0f0]) |> Tables.materializer(tbl)
        @test count_rows(tbl) == 3

        close(tbl); close(conn)
    end

    # ── 3. TableSink creates a new table, returns Table ───────────────────────────
    @testset "TableSink creates table" begin
        conn = Connection(joinpath(tmp, "sink-create"))
        data = (id = Int64[10, 20], score = Float64[1.1, 2.2])
        tbl  = Tables.materializer(TableSink(conn, "new_table"))(data)
        @test tbl isa Table
        @test count_rows(tbl) == 2

        close(tbl); close(conn)
    end

    # ── 4. TableSink pipe syntax ─────────────────────────────────────────────────
    @testset "TableSink pipe syntax" begin
        conn = Connection(joinpath(tmp, "sink-create-pipe"))
        tbl  = (name = ["Alice", "Bob"], val = Int32[1, 2]) |>
               Tables.materializer(TableSink(conn, "piped"))
        @test tbl isa Table
        @test count_rows(tbl) == 2

        close(tbl); close(conn)
    end

    # ── 5. materializer(tbl) works with a non-NamedTuple Tables.jl source ────────
    @testset "materializer works with columntable source" begin
        conn = Connection(joinpath(tmp, "sink-columntable"))
        src  = Tables.columntable((a = Int32[1, 2, 3], b = Float32[0.1f0, 0.2f0, 0.3f0]))
        tbl  = Tables.materializer(TableSink(conn, "from_src"))(src)
        @test count_rows(tbl) == 3

        more = Tables.columntable((a = Int32[4], b = Float32[0.4f0]))
        Tables.materializer(tbl)(more)
        @test count_rows(tbl) == 4

        close(tbl); close(conn)
    end

    # ── 6. materializer can be called multiple times on the same tbl ─────────────
    @testset "materializer called twice accumulates rows" begin
        conn = Connection(joinpath(tmp, "sink-multi"))
        tbl  = create_table(conn, "t",
                             (id = Int32[1], v = Float32[0.0f0]))
        @test count_rows(tbl) == 1

        m = Tables.materializer(tbl)
        m((id = Int32[2], v = Float32[1.0f0]))
        m((id = Int32[3, 4], v = Float32[2.0f0, 3.0f0]))
        @test count_rows(tbl) == 4

        close(tbl); close(conn)
    end

    # ── 7. TableSink with vector column ──────────────────────────────────────────
    @testset "TableSink with vector column" begin
        conn = Connection(joinpath(tmp, "sink-vec"))
        data = (
            id  = ["r1", "r2"],
            vec = [Float32[1, 0, 0], Float32[0, 1, 0]],
        )
        tbl = data |> Tables.materializer(TableSink(conn, "vecs"))
        @test tbl isa Table
        @test count_rows(tbl) == 2

        close(tbl); close(conn)
    end
end
