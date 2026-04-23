# Table operation tests (M4 work — requires Arrow batch construction)
@testset "Table" begin
    # Placeholder: structural checks only until M4 Arrow batch integration.
    @test LanceDB.Table <: Any
    @test LanceDBVectorIndexConfig() isa LanceDBVectorIndexConfig
    @test LanceDBScalarIndexConfig() isa LanceDBScalarIndexConfig
    @test LanceDBFtsIndexConfig() isa LanceDBFtsIndexConfig
    @test LanceDBMergeInsertConfig() isa LanceDBMergeInsertConfig

    # Verify default config values from the spec
    cfg = LanceDBVectorIndexConfig()
    @test cfg.num_partitions  == -1
    @test cfg.num_sub_vectors == -1
    @test cfg.distance_type   == Int32(L2)
end
