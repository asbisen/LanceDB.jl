# Query / Expression DSL tests (M5 / M8 work)
@testset "Query" begin
    # ── Expression DSL ────────────────────────────────────────────────────────
    @testset "LanceDBExpr constructors" begin
        e1 = col("year")
        @test e1 isa LanceDBExpr

        e2 = lit(2020)
        @test e2 isa LanceDBExpr

        e3 = lit("hello")
        @test e3 isa LanceDBExpr

        e4 = lit(3.14)
        @test e4 isa LanceDBExpr

        e5 = lit(true)
        @test e5 isa LanceDBExpr
    end

    @testset "LanceDBExpr binary ops" begin
        e = col("year") > lit(2020)
        @test e isa LanceDBExpr
        @test e._consumed == false

        e2 = col("score") > lit(0.9)
        combined = col("year") > lit(2020)
        combined2 = col("score") > lit(0.5)
        result = combined & combined2
        @test result isa LanceDBExpr
    end

    @testset "LanceDBExpr consumed after binary op" begin
        a = col("x")
        b = lit(1)
        _result = a & b
        @test a._consumed == true
        @test b._consumed == true
    end

    @testset "LanceDBExpr clone allows reuse" begin
        original = col("x")
        cloned   = copy(original)
        @test cloned isa LanceDBExpr
        # original is still alive (not consumed by copy)
        @test original._consumed == false
    end

    @testset "Query and VectorQuery are accessible" begin
        @test LanceDB.Query <: Any
        @test LanceDB.VectorQuery <: Any
        @test LanceDB.QueryResult <: Any
    end
end
