# Vector search tests (M6 work — requires populated table)
@testset "VectorSearch" begin
    # Structural / enum tests only until M6 data-add is implemented.
    @testset "DistanceType enum" begin
        @test L2     == DistanceType(0)
        @test Cosine == DistanceType(1)
        @test Dot    == DistanceType(2)
        @test Hamming == DistanceType(3)
    end

    @testset "IndexType enum" begin
        @test Auto      == IndexType(0)
        @test BTree     == IndexType(1)
        @test IVFFlat   == IndexType(5)
        @test IVFHNSWsq == IndexType(8)
    end

    @testset "make_vector_schema" begin
        schema = make_vector_schema("key", "data", 8)
        @test schema != C_NULL
        release_arrow_schema(schema)
    end

    @testset "make_schema" begin
        schema = make_schema(["id" => "l", "text" => "u"])
        @test schema != C_NULL
        release_arrow_schema(schema)
    end
end
