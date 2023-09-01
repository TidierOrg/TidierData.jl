function safe_getindex(arr, index, default_value="")
    if index <= length(arr)
        return arr[index]
    else
        return default_value
    end
end

function separate(df::DataFrame, col::Symbol, into::Vector{Symbol}, sep::String)
    new_df = df[:, :]
    new_cols = map(x -> split(x, sep), new_df[:, col])
    max_cols = maximum(length.(new_cols))

    if length(into) < max_cols
        error("Not enough names provided in `into` for all split columns.")
    end

    for i in 1:max_cols
        new_df[:, into[i]] = map(x -> safe_getindex(x, i, missing), new_cols)
    end

    new_df = select(new_df, Not(col))

    return new_df
end

"""
$docstring_separate
"""
macro separate(df, from, into, sep)
    from_quoted = QuoteNode(from)
    
    interpolated_into, _, _ = parse_interpolation(into)
    
    if @capture(interpolated_into, (args__,)) || @capture(interpolated_into, [args__])
        args = QuoteNode.(args)
        into_expr = :[$(args...)]
    else
        into_expr = quote
            if typeof($interpolated_into) <: Vector{String}
                Symbol.($interpolated_into)
            else
                $interpolated_into
            end
        end
    end
    
    return quote
        separate($(esc(df)), $(from_quoted), $(into_expr), $(esc(sep)))
    end
end


function unite(df::DataFrame, new_col_name::Symbol, cols::Vector{Symbol}, sep::String="_")
    new_df = df[:, :]
    new_df[:, new_col_name] = [join(skipmissing(row), sep) for row in eachrow(df[:, cols])]
    return new_df
end

"""
$docstring_unite
"""
macro unite(df, new_col, from_cols, sep)
    new_col = QuoteNode(new_col)
    
    if @capture(from_cols, (args__,))
    elseif @capture(from_cols, [args__])
    end
   
    args = QuoteNode.(args)
    var_expr = quote
         unite($(esc(df)), $new_col, [$(args...)], $sep)
    end
end
