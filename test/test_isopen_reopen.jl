@testset "isopen / reopen!" begin
    tmp = mktempdir()

    # ── 1. fresh connection is open ───────────────────────────────────────────
    @testset "new connection isopen" begin
        conn = Connection(joinpath(tmp, "open1"))
        @test Base.isopen(conn)
        close(conn)
    end

    # ── 2. close sets isopen to false ─────────────────────────────────────────
    @testset "close makes isopen false" begin
        conn = Connection(joinpath(tmp, "open2"))
        close(conn)
        @test !Base.isopen(conn)
    end

    # ── 3. double-close is safe ───────────────────────────────────────────────
    @testset "double close is safe" begin
        conn = Connection(joinpath(tmp, "open3"))
        close(conn)
        @test_nowarn close(conn)
        @test !Base.isopen(conn)
    end

    # ── 4. reopen! on a closed connection re-opens it ─────────────────────────
    @testset "reopen! restores open state" begin
        conn = Connection(joinpath(tmp, "reopen1"))
        close(conn)
        @test !Base.isopen(conn)

        reopen!(conn)
        @test Base.isopen(conn)
        close(conn)
    end

    # ── 5. reopen! on an already-open connection is a no-op ───────────────────
    @testset "reopen! on open connection is no-op" begin
        conn = Connection(joinpath(tmp, "reopen2"))
        h_before = conn.handle
        reopen!(conn)
        @test conn.handle === h_before   # same handle, no new connection opened
        close(conn)
    end

    # ── 6. reopen! returns the same Connection object ─────────────────────────
    @testset "reopen! returns conn" begin
        conn = Connection(joinpath(tmp, "reopen3"))
        close(conn)
        result = reopen!(conn)
        @test result === conn
        close(conn)
    end

    # ── 7. operations work after reopen! ─────────────────────────────────────
    @testset "table_names works after reopen!" begin
        path = joinpath(tmp, "reopen4")
        conn = Connection(path)
        tbl  = create_table(conn, "t", (x = Int32[1, 2],))
        close(tbl)
        close(conn)

        reopen!(conn)
        @test "t" in table_names(conn)
        close(conn)
    end

    # ── 8. uri works on both open and closed connection ───────────────────────
    @testset "uri accessible when closed" begin
        path = joinpath(tmp, "uri1")
        conn = Connection(path)
        @test uri(conn) == path
        close(conn)
        @test uri(conn) == path   # still accessible after close
    end

    # ── 9. close → reopen! → close cycle is idempotent ───────────────────────
    @testset "close/reopen cycle repeatable" begin
        conn = Connection(joinpath(tmp, "cycle"))
        for _ in 1:3
            close(conn)
            @test !Base.isopen(conn)
            reopen!(conn)
            @test Base.isopen(conn)
        end
        close(conn)
    end
end
