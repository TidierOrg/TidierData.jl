function summary_stats(df::DataFrame)
    colnames = names(df)
    summary_data = []
    for column in colnames
        col = df[:, column]
        if eltype(col) <: Union{Number, Missing}
            col_nonmissing = collect(skipmissing(col))
            push!(summary_data, (
                column = column,
                min = minimum(col_nonmissing),
                q1 = quantile(col_nonmissing, 0.25),
                median = median(col_nonmissing),
                mean = mean(col_nonmissing),
                q3 = quantile(col_nonmissing, 0.75),
                max = maximum(col_nonmissing),
                non_missing_values = length(col_nonmissing),
                missing_values = count(ismissing, col),
                total_values = length(col),
                unique_values = length(unique(col_nonmissing))
            ))
        else
            col_nonmissing = collect(skipmissing(col))
            push!(summary_data, (
                column = column,
                min = nothing,
                q1 = nothing,
                median = nothing,
                mean = nothing,
                q3 = nothing,
                max = nothing,
                non_missing_values = length(col_nonmissing),
                missing_values = count(ismissing, col),
                total_values = length(col),
                unique_values = length(unique(col_nonmissing))
            ))
        end
    end
    return DataFrame(summary_data)
end

"""
$docstring_summary
"""
macro summary(df, cols...)
    cols = parse_blocks(cols...)
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