module TestTidierData

using TidierData
using Test
using Documenter

DocMeta.setdocmeta!(TidierData, :DocTestSetup, :(using TidierData); recursive=true)

doctest(TidierData)

end

using TidierData
using Test
using DataFrames

test_df = DataFrame(
    label=[1, 1, 2, 2],
    name=["A", "B", "A", "B"],
    num=[1, 2, 3, 4]
)

@testset "TidierData" verbose = true begin
    include("test_pivots.jl")
    include("test_diffs.jl")
end
