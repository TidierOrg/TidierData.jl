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
        if df2 isa GroupedDataFrame
            nr = nrow(transform(df2; ungroup = true))
            nc = ncol(transform(df2; ungroup = true))
        else
            nr = nrow(df2)
            nc = ncol(df2)
        end
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

function log_changed_columns(
    df_copy::AbstractDataFrame,
    df_output::AbstractDataFrame,
    base_msg::String = ""
)

    local changed_msg = ""
    local common_cols = intersect(names(df_copy), names(df_output))

    for c in common_cols
        oldcol = df_copy[!, c]
        newcol = df_output[!, c]

        # Count how many elements changed (ignoring missing→missing as “no change”)
        local n_changed = sum(map((o, n) ->
            (ismissing(o) && ismissing(n)) ? false : coalesce(o != n, true),
            oldcol, newcol))
        if n_changed > 0
            changed_msg *= "Changed $n_changed value(s) in $(c). \n"
        end

        # Track missing deltas
        local old_miss = count(ismissing, oldcol)
        local new_miss = count(ismissing, newcol)
        local delta_miss = new_miss - old_miss
        if delta_miss > 0
            changed_msg *= "Added $delta_miss missing value(s) in $(c). \n"
        elseif delta_miss < 0
            changed_msg *= "Replaced $(-delta_miss) missing value(s) in $(c).\n"
        end
    end
    base_msg = replace(base_msg, "No changes." => "")
    # If no column-level changes, just log the base_msg
    if isempty(changed_msg)
        @info base_msg
    else
        @info base_msg * changed_msg
    end
    return  base_msg * changed_msg
end


function log_join_changes(df1, df_output; 
    join_type::String="@left_join",
)
    ni, ci = nrow(df1), ncol(df1)
    no, co = nrow(df_output), ncol(df_output)

    # Which columns are new in the output that didn't exist in df1?
    new_cols = setdiff(names(df_output), names(df1))

    # Construct a descriptive message
    message = ""
    if !isempty(new_cols)
        message = "$join_type: added $(length(new_cols)) new column(s): $(new_cols).\n"
    end

    message *= 
    """
    - Dimension Change: $ni×$ci -> $no×$co
    """
    @info message
    return message
end