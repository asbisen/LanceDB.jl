using Test
using LanceDB

@testset "LanceDB.jl" begin
    include("test_connection.jl")
    include("test_table.jl")
    include("test_query.jl")
    include("test_vector_search.jl")
    include("test_table_add.jl")
end
