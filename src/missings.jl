"""
$docstring_drop_missing
"""
macro drop_missing(df, exprs...)
  interpolated_exprs = parse_interpolation.(exprs)

  tidy_exprs = [i[1] for i in interpolated_exprs]

  tidy_exprs = parse_tidy.(tidy_exprs)
  num_exprs = length(exprs)
  df_expr = quote
    if $(esc(df)) isa GroupedDataFrame
      local col_names = groupcols($(esc(df)))
      
      # A copy is only needed for grouped dataframes because the copy
      # has to be regrouped because `dropmissing()` does not support
      # grouped data frames.
      local df_copy = DataFrame($(esc(df)))
      if $num_exprs == 0
        dropmissing!(df_copy)
      else
        dropmissing!(df_copy, Cols($(tidy_exprs...)))
      end
      groupby(df_copy, col_names; sort = false) # regroup
    else
      if $num_exprs == 0
        dropmissing($(esc(df)))
      else
        dropmissing($(esc(df)), Cols($(tidy_exprs...)))
      end
    end
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

function fill_missing(df::DataFrame, method::String)
  return fill_missing(df, Symbol.(names(df)), method)
end

function fill_missing(df::DataFrame, columns, method::String)
  new_df = copy(df)
  column_symbols = []
    ### this section here allow it to handle : as a between identifier with columns, as well as negated numbers -3 for columns. 

    for col in columns
      if col isa Integer
          push!(column_symbols, Symbol(names(new_df)[col]))
      elseif col isa AbstractRange
          append!(column_symbols, Symbol.(names(new_df)[collect(col)]))
      elseif typeof(col) <: Between
          # Get the column indices for the Between range
          col_indices = DataFrames.index(new_df)[col]
          append!(column_symbols, Symbol.(names(new_df)[col_indices]))
        elseif col isa InvertedIndex
          # Handle InvertedIndex (negative indexing)
          excluded_col = names(new_df)[col.skip]
          append!(column_symbols, setdiff(names(new_df), [excluded_col]))
      else
          push!(column_symbols, Symbol(col))
      end
  end

  for col_syl in column_symbols
      if method == "down"
          last_observation = new_df[1, col_syl]
          for i in 1:nrow(new_df)
              if ismissing(new_df[i, col_syl])
                  new_df[i, col_syl] = last_observation
              else
                  last_observation = new_df[i, col_syl]
              end
          end
      elseif method == "up"
          next_observation = new_df[end, col_syl]
          for i in nrow(new_df):-1:1
              if ismissing(new_df[i, col_syl])
                  new_df[i, col_syl] = next_observation
              else
                  next_observation = new_df[i, col_syl]
              end
          end
      else
          throw(ArgumentError("Unknown method: $method"))
      end
  end

  return new_df
end

function fill_missing(gdf::GroupedDataFrame, cols, method::String)
  group_cols = groupcols(gdf)
  results = []
  for group in gdf
      # call the DataFrame version of fill_missing on the SubDataFrame
      processed_group = fill_missing(DataFrame(group), cols, method)
      push!(results, processed_group)
  end
  combined_df = vcat(results...)
  return groupby(combined_df, group_cols)
end

"""
$docstring_fill_missing
"""
macro fill_missing(df, args...)
  # Handling the simpler case of only a method provided
  if length(args) == 1
      method = args[1]
      return quote
          if $(esc(df)) isa GroupedDataFrame
              combine($(esc(df))) do gd
                  fill_missing(gd, $method)
              end
          else
              fill_missing($(esc(df)), $method)
          end
      end
  end

interpolated_exprs = parse_interpolation.(args[1:(length(args)-1)])
tidy_exprs = [i[1] for i in interpolated_exprs]
tidy_exprs = parse_tidy.(tidy_exprs)

method = esc(last(args))
cols_quoted = tidy_exprs

  return quote
      if $(esc(df)) isa GroupedDataFrame
          fill_missing($(esc(df)), [$(cols_quoted...)], $method)
      else
          fill_missing($(esc(df)), [$(cols_quoted...)], $method)
      end
  end
end

"""
$docstring_missing_if
"""
missing_if(x, value) = ismissing(x) ? x : (x == value ? missing : x)

"""
$docstring_replace_missing
"""
replace_missing(x, replacement) = ismissing(x) ? replacement : x
