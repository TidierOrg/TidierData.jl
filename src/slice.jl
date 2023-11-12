"""
$docstring_slice
"""
macro slice(df, exprs...)
  exprs = QuoteNode(exprs)
  df_expr = quote
    local interpolated_indices = parse_slice_n.($exprs, nrow(DataFrame($(esc(df)))))
    local original_indices = [eval.(interpolated_indices)...]
    local clean_indices = Int64[]
    for index in original_indices
      if index isa Number
        push!(clean_indices, index)
      else
        append!(clean_indices, collect(index))
      end
    end
    
    if all(clean_indices .> 0)
      if $(esc(df)) isa GroupedDataFrame
        combine($(esc(df)); ungroup = false) do sdf
          sdf[clean_indices, :]
        end
      else
        combine($(esc(df))) do sdf
          sdf[clean_indices, :]
        end
      end
    elseif all(clean_indices .< 0)
      clean_indices = -clean_indices
      if $(esc(df)) isa GroupedDataFrame
        combine($(esc(df)); ungroup = true) do sdf
          sdf[Not(clean_indices), :]
        end
      else
        combine($(esc(df))) do sdf
          sdf[Not(clean_indices), :]
        end
      end
    else
      throw("@slice() indices must either be all positive or all negative.")
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
      @slice($(esc(df)), sample(1:n(), 1; replace=$replace))
    end
  end

  return df_expr
end

"""
$docstring_slice_max
"""
macro slice_max(df, exprs...)
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