# The goal of this benchmarking is to guage how Tidier.jl performs in comparison to DataFrames.jl. Ultimately, from this benchmarking, we can check that Tidier.jl is comparable in speed to DataFrames.jl.

# ## Why function wrap?

# Wrapping code in a function allows it to compile just once, which more closely reflects the reality of production workflows. For a more robust explanation, please see @kdpsingh comment here: [comment](https://github.com/TidierOrg/TidierData.jl/issues/24#issuecomment-1682718061)

using TidierData
using RDatasets
using BenchmarkTools

movies = dataset("ggplot2", "movies");

# ## `filter`
function filtering_tidier()
@chain movies begin
    @filter(Year > 1939 && Votes > 40)
end
end;

# `TidierData.jl` Results
@benchmark filtering_tidier()

# `DataFrames.jl` Results 
@benchmark filter(row -> row.Year > 1939 && row.Votes > 40, movies)

# ## `group_by` `summarize`
function groupbysummarize_tidier()
@chain movies begin
    @group_by(MPAA)
    @summarise(n=n())
end
end;

# `TidierData.jl` Results
@benchmark groupbysummarize_tidier()

# `DataFrames.jl` Results 
@benchmark combine(groupby(movies, :MPAA), nrow => :n)

# ## one `mutate`
function mutate_1_tidier()
@chain movies begin
    @mutate(new_col = Votes * R1)
end
end;

# `TidierData.jl` Results
@benchmark mutate_1_tidier()

# `DataFrames.jl` Results 
@benchmark transform(movies, [:Votes, :R1] => ((v, r) -> v .* r) => :new_col)


# ## `mutate` 6 new columns
function mutate6_tidier()
    @chain movies begin
        @mutate(
        Votes_R1_Product = Votes .* R1, 
        Rating_Year_Ratio = Rating ./ Year, 
        R1_to_R5_Sum = R1 + R2 + R3 + R4 + R5, 
        High_Budget_Flag = if_else(ismissing(Budget), "NA", Budget .> 50000),
        R6_to_R8_Avg = (R6 + R7 + R8) / 3, 
        year_Minus_Length = Year - Length)
    end
end;

# `TidierData.jl` Results 
@benchmark mutate6_tidier()

# `DataFrames.jl` Results 
@benchmark transform(movies, [:Votes, :R1] => ((v, r) -> v .* r) => :Votes_R1_Product, [:Rating, :Year] => ((r, y) -> r ./ y) => :Rating_Year_Ratio, [:R1, :R2, :R3, :R4, :R5] => ((a, b, c, d, e) -> a + b + c + d + e) => :R1_to_R5_Sum, :Budget => (b -> ifelse.(ismissing.(b), missing, b .> 50000)) => :High_Budget_Flag, [:R6, :R7, :R8] => ((f, g, h) -> (f + g + h) / 3) => :R6_to_R8_Avg, [:Year, :Length] => ((y, l) -> y - l) => :Year_Minus_Length )

# ## `groupby` then 2 `mutates`

function groupby1_2mutate_tidier()
@chain movies begin 
    @group_by(MPAA)
    @mutate(ace = R1 -> R1/2 * 4)
    @mutate(Bace = Votes^R1)
end 
end;

# `TidierData.jl` Results
@benchmark groupby1_2mutate_tidier()

# `DataFrames.jl` Results
@benchmark transform( transform( groupby(movies, :MPAA), :R1 => (x -> x/2 * 4) => :ace, ungroup = false), [:Votes, :R1] => ((a, b) -> b .^ a) => :Bace, ungroup = false)

# ## `select` 5 columns
function select5_tidier()
    @chain movies begin 
        @select(R1:R5)
    end 
end;

# `TidierData.jl` Results
@benchmark select5_tidier()

# `DataFrames.jl` Results
@benchmark select(movies, :R1, :R2, :R3, :R4, :R5)
