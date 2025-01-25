function generate_log(df_copy, df_output, name, modes)
    message = ""
    for mode in modes
        message *= mode_message(df_copy, df_output, name, mode)
    end
    if message == ""
        message = "No changes."
    end
    return message
end


function mode_message(df1, df2, name, mode)
    message = ""
    if mode == :colchange
        added = setdiff(names(df2), names(df1))
        rem = setdiff(names(df1), names(df2))
        if length(rem) != 0
            message *= "$name removed: $([r for r in rem]) "
        end
        if length(added) != 0
            message *= "$name added: $([a for a in added]) "
        end
    elseif mode == :rowchange
        row_change = nrow(df2) - nrow(df1)
        if row_change > 0
            message *= "$name added $row_change rows. "
        elseif row_change < 0
            message *= "$name removed $(-row_change) rows. "
        end
    elseif mode == :newsize
        nr = nrow(df2)
        nc = ncol(df2)
        nrs = nr == 1 ? "" : "s"
        ncs = nc == 1 ? "" : "s"
        message *= "$name returned a DataFrame ($nr row$nrs, $nc column$ncs). "
    elseif mode == :groups
        if df2 isa GroupedDataFrame && df1 isa DataFrame
            groups = unique([names(a) for a in collect(keys(df2))])
            message *= "$name added groups: $([g for g in groups][1])"
        elseif df2 isa GroupedDataFrame && df1 isa GroupedDataFrame
            g1 = unique([names(a) for a in collect(keys(df1))])
            g2 = unique([names(a) for a in collect(keys(df2))])
            added = setdiff(g2, g1)
            rem = setdiff(g1, g2)
            if length(added) != 0
                message *= "$name added groups: $([g for g in added][1])"
            end
            if length(rem) != 0
                message *= "$name removed groups: $([g for g in rem][1])"
            end
        elseif df2 isa DataFrame && df1 isa GroupedDataFrame
            g1 = unique([names(a) for a in collect(keys(df1))])
            message *= "$name removed groups: $([g for g in g1][1])"
        end
    end
    return message
end


function changed_columns_log(df_copy::AbstractDataFrame, df_output::AbstractDataFrame, base_msg::String)
    local changed_msg = ""
    local common_cols = intersect(names(df_copy), names(df_output))

    for c in common_cols
        # Old vs new column
        oldcol = df_copy[!, c]
        newcol = df_output[!, c]

        # Count how many elements actually changed, ignoring two missing as “no change”
        local n_changed = sum(map((o, n) ->
            (ismissing(o) && ismissing(n)) ? false : coalesce(o != n, true),
            oldcol, newcol))
        if n_changed > 0
            changed_msg *= " \nChanged $n_changed value(s) in $(c)."
        end

        # Track missing deltas
        local old_miss = count(ismissing, oldcol)
        local new_miss = count(ismissing, newcol)
        local delta_miss = new_miss - old_miss
        if delta_miss > 0
            changed_msg *= " \nAdded $delta_miss missing value(s) in $(c)."
        elseif delta_miss < 0
            changed_msg *= " \nReplaced $(-delta_miss) missing value(s) in  $(c)."
        end
    end

    # If no actual cell-level changes, just info the base_msg
    if isempty(changed_msg)
        @info base_msg
    else
        @info base_msg * changed_msg
    end
end




function generate_log(df_copy, df_output; name::String, modes::Vector{Symbol})
    message = ""
    for mode in modes
        message *= mode_message(df_copy, df_output, name, mode)
    end
    if message == ""
        message = "No changes."
    end
    return message
end
