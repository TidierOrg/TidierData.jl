"""
$docstring_slice
"""
macro slice(df, exprs...)
  exprs = parse_blocks(exprs...)

  interpolated_exprs = parse_interpolation.(exprs; from_slice = true)
  tidy_exprs = [i[1] for i in interpolated_exprs]
  tidy_exprs = parse_tidy.(tidy_exprs; from_slice = true)

  negated = [i[2] for i in tidy_exprs]
  tidy_exprs = [i[1] for i in tidy_exprs]

  df_expr = quote
    local df_copy = $(esc(df)) # not a copy
    
    if df_copy isa GroupedDataFrame
      if all(.!$negated)
        combine(df_copy; ungroup = false) do sdf
          sdf[Iterators.flatten([$(tidy_exprs...)]) |> collect,:]
        end
      elseif all($negated)
        combine(df_copy; ungroup = false) do sdf
            sdf[Iterators.flatten([$(tidy_exprs...)]) |> collect |> Not,:]
          end
      else
        throw("@slice() indices must either be all positive or all negative.")
      end
    else
      if all(.!$negated)
        df_copy[Iterators.flatten([$(tidy_exprs...)]) |> collect,:]
      elseif all($negated)
        df_copy[Iterators.flatten([$(tidy_exprs...)]) |> collect |> Not,:]
      else
        throw("@slice() indices must either be all positive or all negative.")
      end
    end
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

"""
$docstring_slice_sample
"""
macro slice_sample(df, exprs...)
  exprs = parse_blocks(exprs...)

  expr_dict = Dict(begin @capture(expr, lhs_ = rhs_); lhs => rhs end for expr in exprs)
  if haskey(expr_dict, :replace)
    replace = expr_dict[:replace]
  else
    replace = false
  end

  df_expr = quote
    if haskey($expr_dict, :n)
      @slice($(esc(df)), sample(1:n(), $expr_dict[:n]; replace=$replace))
    elseif haskey($expr_dict, :prop)
      @slice($(esc(df)),
        sample(1:n(),
                as_integer(floor(n() * $expr_dict[:prop]));
                replace=$replace))
    else
      throw("Please provide either an `n` or a `prop` value as a keyword argument.")
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
  for expr in exprs
      if @capture(expr, lhs_ = rhs_)
          expr_dict[lhs] = rhs
          if lhs == :missing_rm
              missing_rm = rhs
          elseif lhs == :prop
              arranged = true
          end
      else
          column = expr
      end
  end
  if haskey(expr_dict, :with_ties)
      with_ties = expr_dict[:with_ties]
  end
  if column === nothing
      throw(ArgumentError("No column provided"))
  end
  return quote
      grouping_cols = Symbol[]
      if $(esc(df)) isa DataFrames.GroupedDataFrame
          grouping_cols = DataFrames.groupcols($(esc(df)))
      end
      temp_df = if $arranged
          if $missing_rm
              @chain $(esc(df)) begin 
                  @filter(!ismissing($column))
                  @arrange(desc($column))
              end
          else
              @chain $(esc(df)) begin 
                  @arrange(desc($column))
              end
          end
      else
          @filter($(esc(df)), $column == maximum(skipmissing($column)))
      end
      if temp_df isa DataFrames.GroupedDataFrame
          result_dfs = []
          for sdf in temp_df
              local prop_val
              if haskey($expr_dict, :prop)
                  prop_val = $expr_dict[:prop]
                  if prop_val < 0.0 || prop_val > 1.0
                      throw(ArgumentError("Prop value should be between 0 and 1"))
                  end
                  num_rows = floor(Int, nrow(sdf) * prop_val)
                  push!(result_dfs, first(sdf, num_rows))
              elseif $with_ties
                  push!(result_dfs, sdf)
              else
                  n = haskey($expr_dict, :n) ? $expr_dict[:n] : 1
                  push!(result_dfs, first(sdf, n))
              end
          end
          temp_df = vcat(result_dfs...)
          temp_df = DataFrames.groupby(temp_df, grouping_cols)
      else
          local prop_val
          if haskey($expr_dict, :prop)
              prop_val = $expr_dict[:prop]
              if prop_val < 0.0 || prop_val > 1.0
                  throw(ArgumentError("Prop value should be between 0 and 1"))
              end
              num_rows = floor(Int, nrow(temp_df) * prop_val)
              temp_df = first(temp_df, num_rows)
          elseif !$with_ties
              n = haskey($expr_dict, :n) ? $expr_dict[:n] : 1
              temp_df = first(temp_df, n)
          end
          temp_df
      end
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
  for expr in exprs
      if @capture(expr, lhs_ = rhs_)
          expr_dict[lhs] = rhs
          if lhs == :missing_rm
              missing_rm = rhs
          elseif lhs == :prop
              arranged = true
          end
      else
          column = expr
      end
  end
  if haskey(expr_dict, :with_ties)
      with_ties = expr_dict[:with_ties]
  end
  if column === nothing
      throw(ArgumentError("No column provided"))
  end
  return quote
      grouping_cols = Symbol[]
      if $(esc(df)) isa DataFrames.GroupedDataFrame
          grouping_cols = DataFrames.groupcols($(esc(df)))
      end
      temp_df = if $arranged
          if $missing_rm
              @chain $(esc(df)) begin 
                  @filter(!ismissing($column))
                  @arrange($column)
              end
          else
              @chain $(esc(df)) begin 
                  @arrange($column)
              end
          end
      else
          @filter($(esc(df)), $column == minimum(skipmissing($column)))
      end
      if temp_df isa DataFrames.GroupedDataFrame
          result_dfs = []
          for sdf in temp_df
              local prop_val
              if haskey($expr_dict, :prop)
                  prop_val = $expr_dict[:prop]
                  if prop_val < 0.0 || prop_val > 1.0
                      throw(ArgumentError("Prop value should be between 0 and 1"))
                  end
                  num_rows = floor(Int, nrow(sdf) * prop_val)
                  push!(result_dfs, first(sdf, num_rows))
              elseif $with_ties
                  push!(result_dfs, sdf)
              else
                  n = haskey($expr_dict, :n) ? $expr_dict[:n] : 1
                  push!(result_dfs, first(sdf, n))
              end
          end
          temp_df = vcat(result_dfs...)
          temp_df = DataFrames.groupby(temp_df, grouping_cols)
      else
          local prop_val
          if haskey($expr_dict, :prop)
              prop_val = $expr_dict[:prop]
              if prop_val < 0.0 || prop_val > 1.0
                  throw(ArgumentError("Prop value should be between 0 and 1"))
              end
              num_rows = floor(Int, nrow(temp_df) * prop_val)
              temp_df = first(temp_df, num_rows)
          elseif !$with_ties
              n = haskey($expr_dict, :n) ? $expr_dict[:n] : 1
              temp_df = first(temp_df, n)
          end
          temp_df
      end
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
          throw(ArgumentError("Prop value should be between 0 and 1"))
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
          throw(ArgumentError("Prop value should be between 0 and 1"))
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