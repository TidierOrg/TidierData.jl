# The `@summary()` macro in `TidierData.jl` provides a concise way to compute summary statistics on data. Similar to its R counterpart, it will provide the mean, median, Q1, Q3, minimum, maximum, and number of missing values in a numerical column or columns. 

# ## Summary for the whole dataframe 

using TidierData

df = DataFrame( A = [1, 2, 3, 4, 5], B = [missing, 7, 8, 9, 10], C = [11, missing, 13, 14, missing], D = [16, 17, 18, 19, 20]);

@chain df begin
    @summary()
end

@summary(df)

# ## You can specify columns for which you want to compute the summary. This is useful if the DataFrame has a large number of columns and you're interested in only a subset of them.

@chain df begin
    @summary(B)
end

@summary(df, B)

# ## or for a range of columns

@chain df begin
    @summary(B:D) # you can also write this @summary(2:4)
end