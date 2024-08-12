# TidierData.jl is built on DataFrames.jl. 

# This section will directly compare the two package syntaxes.
# 
# This documentation is based directly off of the DataFrames.jl documentation [comparing different workflows.](https://dataframes.juliadata.org/stable/man/comparisons/#Comparison-with-the-R-package-dplyr)

# To run these examples, use these two dataframes.
# ```julia
# using DataFrames, TidierData # TidierData reexports Statistics.jl which is why its not present in this line
# df = DataFrame(grp=repeat(1:2, 3), x=6:-1:1, y=4:9, z=[3:7; missing], id='a':'f')
# df2 = DataFrame(grp=[1, 3], w=[10, 11])
# ```

# ## Basic Operations
# | Operation                | TidierData.jl                        | DataFrames.jl                          |
# |:-------------------------|:-------------------------------------|:---------------------------------------|
# | Reduce multiple values   | `@summarize(df, mean_x = mean(x))`   | `combine(df, :x => mean)`              |
# | Add new columns          | `@mutate(df, mean_x = mean(x))`      | `transform(df, :x => mean => :x_mean)` |
# | Rename columns           | `@rename(df, x_new = x)`             | `rename(df, :x => :x_new)`             |
# | Pick columns             | `@select(df, x, y)`                  | `select(df, :x, :y)`                   |
# | Pick & transform columns | `@transmute(df, mean_x = mean(x), y)`| `select(df, :x => mean, :y)`           |
# | Pick rows                | `@filter(df, x >= 1)`                | `subset(df, :x => ByRow(x -> x >= 1))` |
# | Sort rows                | `@arrange(df, x)`                    | `sort(df, :x)`                         |
  
# As in DataFrames.jl, some of these functions can operate by group on a grouped dataframe. Below we show TidierData macros nested, although typically, these would be chained together:

# ## Grouped DataFrames
# | Operation                | TidierData.jl                                         | DataFrames.jl                               |
# |:-------------------------|:------------------------------------------------------|:--------------------------------------------|
# | Reduce multiple values   | `@summarize(@group_by(df, grp), mean_x = mean(x))`    | `combine(groupby(df, :grp), :x => mean)`    |
# | Add new columns          | `@mutate(@group_by(df, grp), mean_x = mean(x))`       | `transform(groupby(df, :grp), :x => mean)`  |
# | Pick & transform columns | `@transmute(@group_by(df, grp), mean_x = mean(x), y)` | `select(groupby(df, :grp), :x => mean, :y)` |

# ## More advanced commands are shown below:

# | Operation                 | TidierData.jl                                             | DataFrames.jl                                                              |
# |:--------------------------|:----------------------------------------------------------|:---------------------------------------------------------------------------|
# | Complex Function          | `@summarize(df, mean_x = mean(skipmissing(x)))`           | `combine(df, :x => x -> mean(skipmissing(x)))`                             |
# | Transform several columns | `@summarize(df, x_max = maximum(x), y_min = minimum(y))`  | `combine(df, :x => maximum,  :y => minimum)`                               |
# |                           | `@summarize(df, across((x, y), mean))`                    | `combine(df, [:x, :y] .=> mean)`                                           |
# |                           | `@summarize(df, across(starts_with("x"), mean))`          | `combine(df, names(df, r"^x") .=> mean)`                                   |
# |                           | `@summarize(df, across((x, y), (maximum, minimum)))`      | `combine(df, ([:x, :y] .=> [maximum minimum])...)`                         |
# | DataFrame as output       | `@summarize(df, test = [minimum(x), maximum(x)])`         | `combine(df, :x => (x -> (value = [minimum(x), maximum(x)],)) => AsTable)` |


# ## Joining DataFrames

# | Operation             | TidierData.jl                                   | DataFrames.jl                   |
# |:----------------------|:------------------------------------------------|:--------------------------------|
# | Inner join            | `@inner_join(df, df2, grp)`                     | `innerjoin(df, df2, on = :grp)` |
# | Outer join            | `@outer_join(df, df2, grp)`                     | `outerjoin(df, df2, on = :grp)` |
# | Left join             | `@left_join(df, df2, grp)`                      | `leftjoin(df, df2, on = :grp)`  |
# | Right join            | `@right_join(df, df2, grp)`                     | `rightjoin(df, df2, on = :grp)` |
# | Anti join (filtering) | `@anti_join(df, df2, grp)`                      | `antijoin(df, df2, on = :grp)`  |
# | Semi join (filtering) | `@semi_join(df, df2, grp)`                      | `semijoin(df, df2, on = :grp)`  |

