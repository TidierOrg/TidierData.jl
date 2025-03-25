# TidierData.jl comes with two settings that make it easier to understand the transformations that are being applied to a data frame and to troubleshoot errors. These settings are `log` and `code`. The `log` setting outputs information about the data frame after each transformation, including the number of missing values and the number of unique values in each column. The `code` setting outputs the code that is being executed by the TidierData.jl macros. By default, both settings are set to `false`. This page will review the `log` and `code` settings using the movies dataset.
# 
# We recommend setting the `log` setting to `true` in general, and especially when you are first learning TidierData.jl. This will help you understand how the data frame is being transformed at each step. The `code` setting is useful for debugging errors in TidierData.jl chains.

using TidierData
using RDatasets

movies = dataset("ggplot2", "movies");

# ## `log`
# Logging is set to `false` by default but can enabled as follows:

TidierData_set("log", true)

# When enabled, each macro called will show information about its transformation of the data. Logging can be especially useful to catch silent bugs (those that do not result in an error). 
# 
# When column values are changed, it will report the number new missing values, the percentage of missing values, and the number of unique values.

@chain movies begin 
    @filter(Year > 2000)
    @mutate(Budget_cat = case_when(Budget > 18000 => "high",
                                   Budget > 2000  => "medium",
                                   Budget > 100 => "low",
                                    true => missing))
    @filter(!ismissing(Budget))
    @group_by(Year, Budget_cat)
    @summarize(Avg_Budget = mean(Budget), n = n())
    @ungroup
    @arrange(n)
end

# Logging can also be disabled.

TidierData_set("log", false) # disable logging

# ## `code`
# Code printing is set to `false` by default. Enabling this setting prints the underlying DataFrames.jl code created by TidierData.jl macros. It can be useful for debugging, especially for users who understand DataFrames.jl syntax, or for filing bug reports.

TidierData_set("code", true) # enable macro code output

@chain movies begin 
    @select(Title, Year, Budget)
    @slice_sample(n = 10)
end

# Code printing can also be disabled.

TidierData_set("code", false) # disable macro code output
