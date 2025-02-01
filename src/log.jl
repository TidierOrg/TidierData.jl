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
        for c in added
            df2 = df2 isa GroupedDataFrame ? parent(df2) : df2 
            col = df2[!, c]
          #  coltype = eltype(col)
            nrows = nrow(df2)
            nuniq = length(unique(col))
            nmiss = count(ismissing, col)
            pct_na = nrows > 0 ? round(100 * nmiss / nrows; digits=0) : 0
            message *= "$name: new variable \"$c\" with $nuniq unique values and $pct_na% missing. \n\t"
        end

    elseif mode == :rowchange
        row_change = (df2 isa DataFrames.GroupedDataFrame ? nrow(parent(df2)) : nrow(df2)) -
                    (df1 isa DataFrames.GroupedDataFrame ? nrow(parent(df1)) : nrow(df1))
        if row_change > 0
            base_nrow = df1 isa DataFrames.GroupedDataFrame ? nrow(parent(df1)) : nrow(df1)
            pct = base_nrow == 0 ? 100 : round(100 * row_change / base_nrow; digits=0)
            total_rows = df2 isa DataFrames.GroupedDataFrame ? nrow(parent(df2)) : nrow(df2)
            message *= "$name: added $row_change rows ($pct%), $total_rows rows total. "
        elseif row_change < 0
            n_removed = -row_change
            base_nrow = df1 isa DataFrames.GroupedDataFrame ? nrow(parent(df1)) : nrow(df1)
            pct = base_nrow == 0 ? 100 : round(100 * n_removed / base_nrow; digits=0)
            remaining = df2 isa DataFrames.GroupedDataFrame ? nrow(parent(df2)) : nrow(df2)
            message *= "$name: removed $n_removed rows ($pct%), $remaining rows remaining. "
        end
    elseif mode == :newsize
        type = "DataFrame"
        if df2 isa GroupedDataFrame
            nr = nrow(transform(df2; ungroup=true))
            nc = ncol(transform(df2; ungroup=true))
            type = "GroupedDataFrame"
        else
            nr = nrow(df2)
            nc = ncol(df2)
        end
        nrs = nr == 1 ? "" : "s"
        ncs = nc == 1 ? "" : "s"
        message *= "$name returned a $type ($nr row$nrs, $nc column$ncs). "
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

function log_changed_columns(df_copy, df_output; base_msg::String="", name::String = "@mutate")

    local grouped_copy   = df_copy isa GroupedDataFrame
    local grouped_output = df_output isa GroupedDataFrame
    local dfc = grouped_copy   ? parent(df_copy)   : df_copy
    local dfo = grouped_output ? parent(df_output) : df_output

    changed_msg = ""
    common_cols = intersect(names(dfc), names(dfo))
    nrows       = nrow(dfo)

    # 1) Detect changed values in columns that exist in both DataFrames
    for c in common_cols
        oldcol = dfc[!, c]
        newcol = dfo[!, c]

        local n_changed = sum(map((o, n) ->
            (ismissing(o) && ismissing(n)) ? false : coalesce(o != n, true),
            oldcol, newcol))

        if n_changed > 0
            local pct_changed = nrows > 0 ? round(100 * n_changed / nrows; digits=0) : 0
            local old_miss    = count(ismissing, oldcol)
            local new_miss    = count(ismissing, newcol)
            local missing_diff = new_miss - old_miss

            if missing_diff > 0
                changed_msg *= "$name: changed $n_changed values ($(pct_changed)%) of \"$c\" ($missing_diff new missing)\n\t"
            elseif missing_diff < 0
                changed_msg *= "$name: changed $n_changed values ($(pct_changed)%) of \"$c\" ($(-missing_diff) replaced missing)\n\t"
            else
                changed_msg *= "$name: changed $n_changed values ($(pct_changed)%) of \"$c\"\n\t"
            end
        end
    end

    local added_cols = setdiff(names(dfo), names(dfc))
    for c in added_cols
        local col    = dfo[!, c]
        local nuniq  = length(unique(col))
        local nmiss  = count(ismissing, col)
        local pct_na = nrows > 0 ? round(100 * nmiss / nrows; digits=1) : 0.0
        changed_msg *= "$name: new variable \"$c\" with $nuniq unique values and $pct_na% missing.\n\t"
    end

    # 3) Produce final output
    base_msg = replace(base_msg, "No changes." => "")
    if isempty(changed_msg)
        @info rstrip(base_msg, ['\n', '\t'])
        return base_msg
    else
        local combined_msg = rstrip(base_msg * changed_msg, ['\n', '\t'])
        @info combined_msg
        return combined_msg
    end
end



function log_join_changes(df1, df_output;
    join_type::String="@left_join",
)
    ni, ci = nrow(df1), ncol(df1)
    no, co = nrow(df_output), ncol(df_output)

    new_cols = setdiff(names(df_output), names(df1))

    message = ""
    if !isempty(new_cols)
        message = "$join_type: added $(length(new_cols)) new column(s): $(new_cols)."
    end

    message *= """
               \n\t- Dimension Change: $ni×$ci -> $no×$co
               """
    @info message
    return message
end
