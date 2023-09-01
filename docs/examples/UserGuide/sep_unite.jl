# Follwing the tidyverse syntax, the `@separate()` macro in `TidierData.jl` separates a single column into multiple columns. This is particularly useful for splitting a column containing delimited values into individual columns.

using TidierData

df = DataFrame(a = ["1-1", "2-2", "3-3-3"]);

# ## Separate the "a" column into "b", "c", and "d" columns based on the dash delimiter

@chain df begin
    @separate(a, (b, c, d), "-")
end

# The into columns can also be designated as follows

new_names = ["x$(i)" for i in 1:3]; ### or new_names = ["b", "c", "d"], or new_names = [:b, :c, :d]

@separate(df, a, new_names, "-")

# The `@unite` macro brings together multiple columns into one, separate the characters by a user specified delimiter

# ## Here, the `@unite` macro combines the "b", "c", and "d" columns columns into a single new "new_col" column using the "/" delimiter

df = DataFrame( b = ["1", "2", "3"], c = ["1", "2", "3"], d = [missing, missing, "3"]);

@chain df begin
    @unite(new_col, (b, c, d), "/")
end

