function generate_log(df, df_expr)
    return diff_dataframes(df, df_expr)
end

function diff_dataframes(df1, df2)
    d1 = Dict(k => v for (k, v) in zip(names(df1), eachcol(df1)))
    d2 = Dict(k => v for (k, v) in zip(names(df2), eachcol(df2)))
    diff = deepdiff(d1, d2)
    message = ""
    rem = removed(diff)
    if length(rem) != 0
        message *= "Removed: $([r for r in rem])"
    end
    add = added(diff)
    if length(add) != 0
        message *= "Added: $([a for a in add])"
    end
    ch = changed(diff)
    if length(ch) != 0
        message *= "Changed: $(length(ch)) rows."
    end
    if message == ""
        message = "No changes."
    end
    return message
end
