function summary_stats(df::DataFrame)
    colnames = names(df)
    summary_data = []
    for column in colnames
        col = df[:, column]
        col_nonmissing = collect(skipmissing(col))
        push!(summary_data, (
            Column = column,
            Min = minimum(col_nonmissing),
            Q1 = quantile(col_nonmissing, 0.25),
            Median = median(col_nonmissing),
            Mean = mean(col_nonmissing),
            Q3 = quantile(col_nonmissing, 0.75),
            Max = maximum(col_nonmissing),
            Count = length(col_nonmissing),
            Missing_Count = count(ismissing, col)
        ))
    end
    return DataFrame(summary_data)
end

"""
$docstring_summary
"""
macro summary(df, cols...)
    if length(cols) == 0
        return quote
            summary_stats($(esc(df)))
        end
    else
        selected_cols = [parse_tidy(col) for col in cols]
        return quote
            _selected_df = select($(esc(df)), $(selected_cols...))
            summary_stats(_selected_df)
        end
    end
end