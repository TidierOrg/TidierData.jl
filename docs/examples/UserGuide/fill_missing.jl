using TidierData
using DataFrames


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

# This fills all missing values in the DataFrame with the next available observation.

# ## Fill specifc columns
# This fills missing values in columns `a` and `c` going from bottom to top.

@chain df begin
    @fill_missing((a, c), "up") ### also supports @fill_missing([a, c], up)
end


# ## Fill with Grouped DataFrames
# When grouping by the `group` column, this fills missing values in columns `a` within each group going from top to bottom within that group

@chain df begin
    @group_by(group)
    @fill_missing(a, "down")
end
