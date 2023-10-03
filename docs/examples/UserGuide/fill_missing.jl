# The @fill_missing macro is a reimplementation of fill(). To mirror the syntax in R, the methods availble are "up" (fill from bottom up) and "down" fill from top down.

using TidierData

df = DataFrame(
    a = [missing, 2, 3, missing, 5],
    b = [missing, 1, missing, 4, 5],
    c = ['a', 'b', missing, 'd', 'e'],
    group = ['A', 'A', 'B', 'B', 'A']
);

# ## Fill all columns
# Fill missing values for the whole DataFrame using the "down" method (top to bottom)

@chain df begin
    @fill_missing("down")
end

@fill_missing(df, "down")


# ## Fill specifc columns
# This fills missing values in columns `a` and `c` going from bottom to top.

@chain df begin
    @fill_missing(a, c, "up")
end

# ## Fill with Grouped DataFrames
# When grouping by the `group` column, this fills missing values in columns `a` within each group going from top to bottom within that group

@chain df begin
    @group_by(group)
    @fill_missing(a, "down")
end

# ## `replace_missing()`
# The `replace_missing` function facilitates the replacement of `missing` values with a specified replacement. 

@chain df begin
    @mutate(b = replace_missing(b, 2))
end 

# ## `missing_if()`
# The `missing_if` function is used to introduce `missing` values under specific conditions. 

@chain df begin
    @mutate(b = missing_if(b, 5))
end 

# Both `missing_if` and `replace_missing` are not type specifc.