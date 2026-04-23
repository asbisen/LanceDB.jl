@testset "Table add (M4)" begin
    tmp = mktempdir()

    # ── 1. add rows to empty table, verify count ──────────────────────────────
    @testset "add vector rows, count_rows increases" begin
        conn = Connection(joinpath(tmp, "add-vec"))
        schema = make_vector_schema("id", "vec", 4)
        tbl    = create_table(conn, "t", schema)
        release_arrow_schema(schema)

        @test count_rows(tbl) == 0

        data = (
            id  = ["a", "b", "c"],
            vec = [Float32[1, 0, 0, 0],
                   Float32[0, 1, 0, 0],
                   Float32[0, 0, 1, 0]],
        )
        add(tbl, data)
        @test count_rows(tbl) == 3

        # add more rows
        add(tbl, (id=["d"], vec=[Float32[0,0,0,1]]))
        @test count_rows(tbl) == 4

        close(tbl)
        close(conn)
    end

    # ── 2. create_table directly from data (schema inferred) ──────────────────
    @testset "create_table from data infers schema" begin
        conn = Connection(joinpath(tmp, "add-infer"))
        data = (
            key    = Int64[1, 2, 3],
            value  = Float32[0.1f0, 0.2f0, 0.3f0],
            label  = ["x", "y", "z"],
        )
        tbl = create_table(conn, "t", data)
        @test tbl isa Table
        @test count_rows(tbl) == 3
        v = table_version(tbl)
        @test v >= 1
        close(tbl)
        close(conn)
    end

    # ── 3. add to a table with mixed numeric types ─────────────────────────────
    @testset "add numeric columns" begin
        conn = Connection(joinpath(tmp, "add-num"))
        schema = make_schema(["i32" => "i", "i64" => "l", "f32" => "f", "f64" => "g"])
        tbl    = create_table(conn, "nums", schema)
        release_arrow_schema(schema)

        rows = (
            i32 = Int32[1, 2],
            i64 = Int64[10, 20],
            f32 = Float32[1.5f0, 2.5f0],
            f64 = Float64[3.14, 2.71],
        )
        add(tbl, rows)
        @test count_rows(tbl) == 2

        close(tbl)
        close(conn)
    end
end
