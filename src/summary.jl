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

### And filled

function isgrouped(df::Any)
    return typeof(df) == GroupedDataFrame
end
function fill_missing(df::DataFrame, method::String)
    return fill_missing(df, Symbol.(names(df)), method)
end
function fill_missing(df::DataFrame, cols::Vector{Symbol}, method::String)
    new_df = copy(df)
    
    for col in cols
        if method == "down"
            last_observation = new_df[1, col]
            for i in 1:size(new_df, 1)
                if ismissing(new_df[i, col])
                    new_df[i, col] = last_observation
                else
                    last_observation = new_df[i, col]
                end
            end
        elseif method == "up"
            next_observation = new_df[end, col]
            for i in size(new_df, 1):-1:1
                if ismissing(new_df[i, col])
                    new_df[i, col] = next_observation
                else
                    next_observation = new_df[i, col]
                end
            end
        else
            throw(ArgumentError("Unknown method: $method"))
        end
    end

    return new_df
end

function fill_missing(gdf::GroupedDataFrame, cols::Vector{Symbol}, method::String)
    group_cols = groupcols(gdf)
    results = []
    for group in gdf
        processed_group = fill_missing(DataFrame(group), cols, method)
        push!(results, processed_group)
    end
    combined_df = vcat(results...)
    return groupby(combined_df, group_cols)
end

"""
docstring_fill_missing
"""
macro fill_missing(df, args...)
    if length(args) == 1  # Only method is provided
        method = args[1]
        return quote
            if isgrouped($(esc(df)))
                combine(gd -> fill_missing(gd, $(esc(method))), $(esc(df)))
            else
                fill_missing($(esc(df)), $(esc(method)))
            end
        end
    end

    # Extract columns and method
    cols, method = args[1], args[2]

    if @capture(cols, (args__,))
        # args captured
    elseif @capture(cols, [args__])
        # args captured
    else
        throw(ArgumentError("Expected a tuple or array for columns"))
    end

    args_quoted = QuoteNode.(args)
    
    # Construct the function call expression with columns
    var_expr = quote
        if isgrouped($(esc(df)))
            combine(gd -> fill_missing(gd, [$(args_quoted...)], $(esc(method))), $(esc(df)))
        else
            fill_missing($(esc(df)), [$(args_quoted...)], $(esc(method)))
        end
    end
    return var_expr
end

