# ## `@unnest_longer`

# `@unnest_longer` adds one row per entry of an array or dataframe, lengthening dataframe by flattening the column or columns. 

using TidierData
df = DataFrame(x = 1:4, y = [[], [1, 2, 3], [4, 5], Int[]]);

@chain df begin
    @unnest_longer(y)
end

# If there are rows with empty arrays, `keep_empty` will prevent these rows from being dropped. `include_indices` will add a new column for each flattened column that logs the position of each entry in the array.

@chain df begin
    @unnest_longer(y, keep_empty = true, indices_include = true)
end

# ## `@unnest_wider`

# `@unnest_wider` will widen a column or column(s) of Dicts, Arrays, Tuples or Dataframes into multiple columns.

df2 = DataFrame(
           name = ["Zaki", "Farida"],
           attributes = [
               Dict("age" => 25, "city" => "New York"),
               Dict("age" => 30, "city" => "Los Angeles")]);

@chain df2 begin
    @unnest_wider(attributes)
end


# ## Unnesting nested Dataframes with different lengths which contains arrays

df3 = DataFrame(
    x = 1:3,
    y = Any[
        DataFrame(),
        DataFrame(a = ["A"], b = [14]),
        DataFrame(a = ["A", "B", "C"], b = [13, 12, 11], c = [4, 4, 4])
    ]
)
# `df3` contains dataframes in with different widths that also contain arrays. Chaining together `@unnest_wider` and `@unnest_longer` will unnest the columns to tuples first and then they will be fully unnested after.

@chain df3 begin 
    @unnest_wider(y)
    @unnest_longer(a:c, keep_empty = true)
end


# ## `@nest`

# Nest columns into a dataframe nested into a new column

df4 = DataFrame(x = ["a", "b", "a", "b", "C", "a"], y = 1:6, yz = 13:18, a = 7:12, ab = 12:-1:7)

@nest(df4, n2 = starts_with("y"), n3 = a:ab)