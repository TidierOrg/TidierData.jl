# TidierData comes with  the ability to log changes to the dataframe as operations/transformations are chained together, in addition to the ability to print the DataFrames.jl code underlying the many TidierData macros.
# This page will review the `log` and `code` settings using the movies dataset.

using TidierData
using RDatasets

movies = dataset("ggplot2", "movies");

# ## log
# Logging is set to `false` by default, but is toggled on and off as follows:
TidierData_set("log", true)
# When enabled, each macro called will show information regarding each subsequent transformation of the data. 
# Logging can be especially useful to catch silent bugs. 
# When column values are changed, it will report how many new missing values there, the percent missing, and the number of unique values in that column as well. 
@chain movies begin 
    @filter (Year > 2000)
    @mutate(Budget_cat = case_when(Budget > 18000 => "high",
                                   Budget > 2000  => "medium",
                                   Budget > 100 => "low",
                                    true => missing))
    @filter(!ismissing(Budget))
    @group_by(Year, Budget_cat)
    @summarize(Avg_Budget = mean(Budget), n = n())
    @ungroup
    @arrange n
end

# ## code
# The code printing ability is also set to `false` by default. Its primary value would be for debugging errors within Tidier chains
TidierData_set("code", true)

@chain movies begin 
    @select(Title, Year, Budget)
    @slice_sample n = 10
end