# ## `@unnest_longer`

# `@unnest_longer` adds one row per entry of an array, lengthening dataframe by flattening the column or columns. 

using TidierData
df = DataFrame(x = 1:4, y = [[], [1, 2, 3], [4, 5], Int[]]);

@chain df begin
    @unnest_longer(y)
end

# If there are rows with empty arrays, `keep_empty` will prevent these rows from being dropped. `include_indices` will add a new column for each flattened column that logs the position of each entry in the array.

@chain df begin
    @unnest_longer(y, keep_empty = true, indices_include = true)
end

# ## @unnest_wider

# `@unnest_wider` will widen a column of Dicts or a column(s) of arrays into multiple columns.

df2 = DataFrame(
           name = ["Zaki", "Farida"],
           attributes = [
               Dict("age" => 25, "city" => "New York"),
               Dict("age" => 30, "city" => "Los Angeles")]);

@chain df2 begin
    @unnest_wider(attributes)
end
