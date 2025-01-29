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
df = DataFrame(
                 dt1 = [missing, 0.2, missing, missing, 1, missing, 5, 6],
                 dt2 = [0.3, 2, missing, 3, missing, 5, 6,missing])

@testset "TidierData" verbose = true begin
    include("test_pivots.jl")
    include("test_diffs.jl")
end
