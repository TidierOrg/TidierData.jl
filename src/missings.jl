"""
$docstring_drop_missing
"""
macro drop_missing(df, exprs...)
  exprs = parse_blocks(exprs...)
  interpolated_exprs = parse_interpolation.(exprs)

  tidy_exprs = [i[1] for i in interpolated_exprs]
  tidy_exprs = parse_tidy.(tidy_exprs)
  num_exprs = length(exprs)

  df_expr = quote
      local df_output = if $(esc(df)) isa GroupedDataFrame
          # For a grouped dataframe, we must copy then call dropmissing!
          local col_names = groupcols($(esc(df)))
          local df_copy   = DataFrame($(esc(df)))

          if $num_exprs == 0
              dropmissing!(df_copy)
          else
              dropmissing!(df_copy, Cols($(tidy_exprs...)))
          end

          # Re-group after dropping missing
          groupby(df_copy, col_names; sort = false)
      else
          # For a regular dataframe, we can just dropmissing directly
          if $num_exprs == 0
              dropmissing($(esc(df)))
          else
              dropmissing($(esc(df)), Cols($(tidy_exprs...)))
          end
      end

      # Logging (optional) — compares the original df vs. df_output
      if log[]
          @info generate_log($(esc(df)), df_output, "@drop_missing", [:rowchange])
      end

      df_output
  end

  if code[]  # If you want to pretty-print the generated expression for debugging
      @info MacroTools.prettify(df_expr)
  end

  return df_expr
end


function fill_missing(df::DataFrame, method::String)
  return fill_missing(df, Symbol.(names(df)), method)
end

function fill_missing(df::DataFrame, columns, method::String)
  new_df = copy(df)
  cols_expr = columns isa Expr ? (columns,) : columns
  column_symbols = names(df, Cols(cols_expr...)) 
  for col_sym in column_symbols
      if method == "down"
          last_observation = new_df[1, col_sym]
          for i in 1:nrow(new_df)
              if ismissing(new_df[i, col_sym])
                  new_df[i, col_sym] = last_observation
              else
                  last_observation = new_df[i, col_sym]
              end
          end
      elseif method == "up"
          next_observation = new_df[end, col_sym]
          for i in nrow(new_df):-1:1
              if ismissing(new_df[i, col_sym])
                  new_df[i, col_sym] = next_observation
              else
                  next_observation = new_df[i, col_sym]
              end
          end
      else
          throw(ArgumentError("Unknown method: $method"))
      end
  end

  return new_df
end

function fill_missing(gdf::GroupedDataFrame, columns, method::String)
  group_cols = groupcols(gdf)
  results = []
  cols_expr = columns isa Expr ? (columns,) : columns
  column_symbols = names(gdf, Cols(cols_expr...)) 
  for group in gdf
      # call the DataFrame version of fill_missing on the SubDataFrame
      processed_group = fill_missing(DataFrame(group), column_symbols, method)
      push!(results, processed_group)
  end
  combined_df = vcat(results...)
  return groupby(combined_df, group_cols)
end

"""
$docstring_fill_missing
"""
macro fill_missing(df, args...)
  args = parse_blocks(args...)

  # Handling the simpler case of only a method provided
  if length(args) == 1
      method = args[1]
      return quote
          local df_output = if $(esc(df)) isa GroupedDataFrame
              combine($(esc(df))) do gd
                  fill_missing(gd, $method)
              end
          else
              fill_missing($(esc(df)), $method)
          end

          if log[]
              local base_msg = generate_log($(esc(df)), df_output, "@fill_missing", [:colchange])
              log_changed_columns($(esc(df)), df_output; base_msg, name="@fill_missing")
          end

          df_output
      end
  end

  interpolated_exprs = parse_interpolation.(args[1:(length(args)-1)])
  tidy_exprs = [i[1] for i in interpolated_exprs]
  tidy_exprs = parse_tidy.(tidy_exprs)

  method = esc(last(args))
  cols_quoted = tidy_exprs

  return quote
      local df_output = if $(esc(df)) isa GroupedDataFrame
          fill_missing($(esc(df)), [$(cols_quoted...)], $method)
      else
          fill_missing($(esc(df)), [$(cols_quoted...)], $method)
      end

      if log[]
          local base_msg = generate_log($(esc(df)), df_output, "@fill_missing", [:colchange])
          log_changed_columns($(esc(df)), df_output; base_msg, name= "@fill_missing")
      end

      df_output
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
