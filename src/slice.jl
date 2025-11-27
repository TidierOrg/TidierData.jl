"""
$docstring_slice
"""
macro slice(df, exprs...)
    exprs = parse_blocks(exprs...)

    # 1. FIX: Use collect() and parse_interpolation like in @mutate
    interpolated_exprs_full = collect(parse_interpolation.(exprs; from_slice = true)) # <-- FIX HERE 🔥

    # 2. Use the new function to separate standard expressions from the _by expression
    slice_exprs_parsed, group_expr = parse_expressions_for_by(interpolated_exprs_full)
    
    # 3. Update the unpacking logic to use the filtered array
    tidy_exprs = [i[1] for i in slice_exprs_parsed]
    
    tidy_exprs = parse_tidy.(tidy_exprs; from_slice = true)
  
    negated = [i[2] for i in tidy_exprs]
    tidy_exprs = [i[1] for i in tidy_exprs]
  
    df_expr = quote
      local orig_df = $(esc(df))
      local was_grouped_by_pipe = orig_df isa GroupedDataFrame # Track initial state
      local df_copy = orig_df

      # NEW: Inject Grouping Logic if _by was provided
      $(if !isnothing(group_expr)
          # group_expr is already interpolated.
          parsed_group = parse_group_by(group_expr)

          quote
              # If _by is used, it overrides/sets the grouping
              df_copy = groupby(orig_df, $(esc(parsed_group)); sort = false)
          end
      else
          nothing
      end)
      # END NEW GROUPING

      if df_copy isa GroupedDataFrame
        local should_ungroup = df_copy isa GroupedDataFrame && !was_grouped_by_pipe
        
        if all(.!$negated)
          local df_output = combine(df_copy; ungroup = should_ungroup) do sdf
            sdf[Iterators.flatten([$(tidy_exprs...)]) |> collect,:]
          end
        elseif all($negated)
          local df_output = combine(df_copy; ungroup = should_ungroup) do sdf
              sdf[Iterators.flatten([$(tidy_exprs...)]) |> collect |> Not,:]
            end
        else
          throw("@slice() indices must either be all positive or all negative.") # COV_EXCL_LINE
        end
        
        if log[]
            @info generate_log(orig_df, df_output, "@slice", [:rowchange])
          #  log_changed_columns(orig_df, df_output; base_msg)
        end
        df_output
        
      else
        if all(.!$negated)
          local df_output = df_copy[Iterators.flatten([$(tidy_exprs...)]) |> collect,:]
        elseif all($negated)
          local df_output = df_copy[Iterators.flatten([$(tidy_exprs...)]) |> collect |> Not,:]
        else
          throw("@slice() indices must either be all positive or all negative.") # COV_EXCL_LINE
        end
        
        # 🛑 FIX 1: Use df_output for logging, comparing against orig_df
        log[] && @info generate_log(orig_df, df_output, "@slice", [:rowchange]) 
        
        # 🛑 FIX 2: Return df_output
        df_output
      end
    end
    if code[]
      @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
    end
    return df_expr
end


"""
$docstring_slice_sample
"""
macro slice_sample(df, exprs...)
    exprs = parse_blocks(exprs...)
  
    n_val = nothing
    prop_val = nothing
    replace_val = false
    passthrough_exprs = []
  
    # Parse expressions and separate recognized kwargs from passthrough args (like _by = a)
    for expr in exprs
      if @capture(expr, n = val_)
        n_val = val
      elseif @capture(expr, prop = val_)
        prop_val = val
      elseif @capture(expr, replace = val_)
        replace_val = val
      else
        # Collect all other expressions (like _by = a) to pass to @slice
        push!(passthrough_exprs, expr)
      end
    end
  
    df_expr = quote
      if !isnothing($n_val)
        # Pass the main slicing expression and splat passthrough_exprs (like _by = a)
        @slice($(esc(df)), sample(1:n(), $(esc(n_val)); replace=$(esc(replace_val))), $(passthrough_exprs...))
      elseif !isnothing($prop_val)
        @slice($(esc(df)),
          sample(1:n(),
                  as_integer(floor(n() * $(esc(prop_val))));
                  replace=$(esc(replace_val))),
          $(passthrough_exprs...))
      else
        throw("Please provide either an `n` or a `prop` value as a keyword argument.") # COV_EXCL_LINE
      end
    end
  
    return df_expr
  end
  

"""
$docstring_slice_max
"""
macro slice_max(df, exprs...)
    exprs = parse_blocks(exprs...)

    expr_dict = Dict()
    column = nothing
    missing_rm = true
    with_ties = true
    arranged = false
    n = 1  # default value for n

    for expr in exprs
        if @capture(expr, lhs_ = rhs_)
            expr_dict[lhs] = rhs
            if lhs == :missing_rm
                missing_rm = rhs
            elseif lhs == :prop
                arranged = true
            elseif lhs == :n
                n = rhs  # Capture n if provided
            end
        else
            column = expr
        end
    end

    if haskey(expr_dict, :with_ties)
        with_ties = expr_dict[:with_ties]
    end

    if column === nothing
        throw(ArgumentError("No column provided")) # COV_EXCL_LINE
    end

    return quote
        grouping_cols = Symbol[]
        if $(esc(df)) isa DataFrames.GroupedDataFrame
            grouping_cols = DataFrames.groupcols($(esc(df)))
        end
        temp_df = if $missing_rm
            filter(row -> !ismissing(row isa DataFrameRow ? row[$(QuoteNode(column))] : 
            row[!, $(QuoteNode(column))]), $(esc(df))) 
        else 
            $(esc(df))
        end

        if temp_df isa DataFrames.GroupedDataFrame
        result_dfs = []
        for sdf in temp_df
            max_val = maximum(skipmissing(sdf[!, $(QuoteNode(column))]))
            max_value_rows = nrow(filter(row -> row[$(QuoteNode(column))] == max_val, sdf))
            selected_df = if haskey($expr_dict, :prop)
                prop_val = $expr_dict[:prop]
                if prop_val < 0.0 || prop_val > 1.0
                    throw(ArgumentError("Prop value should be between 0 and 1")) # COV_EXCL_LINE
                end
                num_rows = floor(Int, nrow(sdf) * prop_val)
                if $with_ties && num_rows > max_value_rows
                    first(@arrange(sdf, desc($column)), num_rows)
                elseif $with_ties && num_rows < max_value_rows
                    first(@arrange(sdf, desc($column)), max_value_rows)
                else
                    first(@arrange(sdf, desc($column)), num_rows)
                end
            else
                if $with_ties && $n > max_value_rows
                    first(@arrange(sdf, desc($column)), $n)
                elseif $with_ties && $n < max_value_rows && $n != 1
                    first(@arrange(sdf, desc($column)), max_value_rows)
                elseif $with_ties && $n < max_value_rows && $n == 1
                    first(@arrange(sdf, desc($column)), max_value_rows)
                elseif !$with_ties && $n < max_value_rows 
                    first(@arrange(sdf, desc($column)), $n)
                else
                    first(@arrange(sdf, desc($column)), $n)
                end
            end
            push!(result_dfs, selected_df)
        end
        temp_df = vcat(result_dfs...)
        temp_df = DataFrames.groupby(temp_df, grouping_cols)
    else
        max_val_temp = maximum(skipmissing(temp_df[!, $(QuoteNode(column))]))   
        max_value_rows = nrow(filter(row -> row[$(QuoteNode(column))] == max_val_temp, temp_df))

        temp_df = if haskey($expr_dict, :prop)
            prop_val = $expr_dict[:prop]
            if prop_val < 0.0 || prop_val > 1.0
                throw(ArgumentError("Prop value should be between 0 and 1")) # COV_EXCL_LINE
            end
            num_rows = floor(Int, nrow(temp_df) * prop_val)
            if $with_ties && num_rows > max_value_rows
                first(@arrange(temp_df, desc($column)), num_rows)
            elseif $with_ties && num_rows < max_value_rows
                first(@arrange(temp_df, desc($column)), max_value_rows)
            else
                first(@arrange(temp_df, desc($column)), num_rows)
            end
        else
            if $with_ties && $n > max_value_rows
                first(@arrange(temp_df, desc($column)), $n)
            elseif $with_ties && $n < max_value_rows && $n != 1
                first(@arrange(temp_df, desc($column)), max_value_rows)
            elseif $with_ties && $n < max_value_rows && $n == 1
                first(@arrange(temp_df, desc($column)), max_value_rows)
            else !$with_ties && $n < max_value_rows 
                first(@arrange(temp_df, desc($column)), $n)
            end
        end
    end
        log[] && @info generate_log($(esc(df)), temp_df, "@slice_max", [:rowchange])
        temp_df
    end

end


"""
$docstring_slice_min
"""
macro slice_min(df, exprs...)
    exprs = parse_blocks(exprs...)

    expr_dict = Dict()
    column = nothing
    missing_rm = true
    with_ties = true
    arranged = false
    n = 1  # default value for n

    for expr in exprs
        if @capture(expr, lhs_ = rhs_)
            expr_dict[lhs] = rhs
            if lhs == :missing_rm
                missing_rm = rhs
            elseif lhs == :prop
                arranged = true
            elseif lhs == :n
                n = rhs  # Capture n if provided
            end
        else
            column = expr
        end
    end

    if haskey(expr_dict, :with_ties)
        with_ties = expr_dict[:with_ties]
    end

    if column === nothing
        throw(ArgumentError("No column provided")) # COV_EXCL_LINE
    end

    return quote
        grouping_cols = Symbol[]
        if $(esc(df)) isa DataFrames.GroupedDataFrame
            grouping_cols = DataFrames.groupcols($(esc(df)))
        end
        temp_df = if $missing_rm
            filter(row -> !ismissing(row isa DataFrameRow ? row[$(QuoteNode(column))] : 
            row[!, $(QuoteNode(column))]), $(esc(df))) 
        else 
            $(esc(df))
        end

       if temp_df isa DataFrames.GroupedDataFrame
        result_dfs = []
        for sdf in temp_df
            max_val = minimum(skipmissing(sdf[!, $(QuoteNode(column))]))
            max_value_rows = nrow(filter(row -> row[$(QuoteNode(column))] == max_val, sdf))
            selected_df = if haskey($expr_dict, :prop)
                prop_val = $expr_dict[:prop]
                if prop_val < 0.0 || prop_val > 1.0
                    throw(ArgumentError("Prop value should be between 0 and 1")) # COV_EXCL_LINE
                end
                num_rows = floor(Int, nrow(sdf) * prop_val)
                if $with_ties && num_rows > max_value_rows
                    first(@arrange(sdf, ($column)), num_rows)
                elseif $with_ties && num_rows < max_value_rows
                    first(@arrange(sdf, ($column)), max_value_rows)
                else
                    first(@arrange(sdf, ($column)), num_rows)
                end
            else
                if $with_ties && $n > max_value_rows
                    first(@arrange(sdf, ($column)), $n)
                elseif $with_ties && $n < max_value_rows && $n != 1
                    first(@arrange(sdf, ($column)), max_value_rows)
                elseif $with_ties && $n < max_value_rows && $n == 1
                    first(@arrange(sdf, ($column)), max_value_rows)
                elseif !$with_ties && $n < max_value_rows 
                    first(@arrange(sdf, ($column)), $n)
                else
                    first(@arrange(sdf, ($column)), $n)
                end
            end
            push!(result_dfs, selected_df)
        end
        temp_df = vcat(result_dfs...)
        temp_df = DataFrames.groupby(temp_df, grouping_cols)
    else
        max_val_temp = minimum(skipmissing(temp_df[!, $(QuoteNode(column))]))   
        max_value_rows = nrow(filter(row -> row[$(QuoteNode(column))] == max_val_temp, temp_df))
        temp_df = if haskey($expr_dict, :prop)
            prop_val = $expr_dict[:prop]
            if prop_val < 0.0 || prop_val > 1.0
                throw(ArgumentError("Prop value should be between 0 and 1"))# COV_EXCL_LINE
            end
            num_rows = floor(Int, nrow(temp_df) * prop_val)
            if $with_ties && num_rows > max_value_rows
                first(@arrange(temp_df, ($column)), num_rows)
            elseif $with_ties && num_rows < max_value_rows
                first(@arrange(temp_df, ($column)), max_value_rows)
            else
                first(@arrange(temp_df, ($column)), num_rows)
            end
        else
            if $with_ties && $n > max_value_rows
                first(@arrange(temp_df, ($column)), $n)
            elseif $with_ties && $n < max_value_rows && $n != 1
                first(@arrange(temp_df, ($column)), max_value_rows)
            elseif $with_ties && $n < max_value_rows && $n == 1
                first(@arrange(temp_df, ($column)), max_value_rows)
            else !$with_ties && $n < max_value_rows 
                first(@arrange(temp_df, ($column)), $n)
            end
        end
    end
        log[] && @info generate_log($(esc(df)), temp_df, "@slice_min", [:rowchange]) 
        temp_df
    end

end

"""
$docstring_slice_head
"""
macro slice_head(df, exprs...)
  exprs = parse_blocks(exprs...)

  expr_dict = :(Dict())

  for expr in exprs
      if @capture(expr, lhs_ = rhs_)
          push!(expr_dict.args, :($(QuoteNode(lhs)) => $(esc(rhs))))
      end
  end
  return quote
      expr_dict = $expr_dict
      temp_df = $(esc(df))
      grouping_cols = Symbol[]

      if temp_df isa DataFrames.GroupedDataFrame
          grouping_cols = DataFrames.groupcols(temp_df)
      end
      local n = get(expr_dict, :n, 1)
      local prop_val = get(expr_dict, :prop, 1.0) 
      if prop_val < 0.0 || prop_val > 1.0
          throw(ArgumentError("Prop value should be between 0 and 1")) # COV_EXCL_LINE
      end
      if temp_df isa DataFrames.GroupedDataFrame
          result_dfs = []
          for sdf in temp_df
              local group_n = n
              if prop_val != 1.0
                  group_n = floor(Int, nrow(sdf) * prop_val)
              end
              push!(result_dfs, first(sdf, group_n))
          end
          temp_df = vcat(result_dfs...)
      else
          if prop_val != 1.0
              n = floor(Int, nrow(temp_df) * prop_val)
          end
          temp_df = first(temp_df, n)
      end

      if !isempty(grouping_cols)
          temp_df = DataFrames.groupby(temp_df, grouping_cols)
      end
      temp_df
  end
end

"""
$docstring_slice_tail
"""
macro slice_tail(df, exprs...)
  exprs = parse_blocks(exprs...)
  
  expr_dict = :(Dict())
  for expr in exprs
      if @capture(expr, lhs_ = rhs_)
          push!(expr_dict.args, :($(QuoteNode(lhs)) => $(esc(rhs))))
      end
  end
  return quote
      expr_dict = $expr_dict
      temp_df = $(esc(df))
      grouping_cols = Symbol[]
      if temp_df isa DataFrames.GroupedDataFrame
          grouping_cols = DataFrames.groupcols(temp_df)
      end
      local n = get(expr_dict, :n, 1) 
      local prop_val = get(expr_dict, :prop, 1.0) 
      if prop_val < 0.0 || prop_val > 1.0
          throw(ArgumentError("Prop value should be between 0 and 1")) # COV_EXCL_LINE
      end
      if temp_df isa DataFrames.GroupedDataFrame
          result_dfs = []
          for sdf in temp_df
              local group_n = n
              if prop_val != 1.0
                  group_n = floor(Int, nrow(sdf) * prop_val)
              end
              push!(result_dfs, last(sdf, group_n))
          end
          temp_df = vcat(result_dfs...)
      else
          if prop_val != 1.0
              n = floor(Int, nrow(temp_df) * prop_val)
          end
          temp_df = last(temp_df, n)
      end

      if !isempty(grouping_cols)
          temp_df = DataFrames.groupby(temp_df, grouping_cols)
      end
      temp_df
  end
end