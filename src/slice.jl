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