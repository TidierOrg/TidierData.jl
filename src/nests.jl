function unnest_wider(df::Union{DataFrame, GroupedDataFrame}, cols; names_sep::Union{String, Nothing}=nothing)
  is_grouped = df isa GroupedDataFrame
  grouping_columns = is_grouped ? groupcols(df) : Symbol[]
  # Ungroup if necessary
  df_copy = copy(is_grouped ? parent(df) : df)
  # getting column names from parse tidy
  cols_expr = cols isa Expr ? (cols,) : cols
  column_symbols = names(df_copy, Cols(cols_expr...)) 

  for col in column_symbols
      col_type = typeof(df_copy[1, col])
      if col_type <: Dict
          keys_set = Set{String}()
          for item in df_copy[!, col]
              union!(keys_set, keys(item))
          end

          for key in keys_set
              new_col_name = names_sep === nothing ? Symbol(key) : Symbol(string(col, names_sep, key))
              df_copy[!, new_col_name] = getindex.(df_copy[!, col], key)
          end
      elseif col_type <: Array
          n = length(first(df_copy[!, col]))
          for i in 1:n
              new_col_name = names_sep === nothing ? Symbol(string(col, i)) : Symbol(string(col, names_sep, i))
              df_copy[!, new_col_name] = getindex.(df_copy[!, col], i)
          end
      else
          error("Column $col contains neither dictionaries nor arrays")
      end
      select!(df_copy, Not(col))
  end
   if is_grouped
    df_copy = groupby(df_copy, grouping_columns)
   end
  return df_copy
end

"""
$docstring_unnest_wider
"""
macro unnest_wider(df, exprs...)
  names_sep = :(nothing) 
  if length(exprs) >= 2 && isa(exprs[end], Expr) && exprs[end].head == :(=) && exprs[end].args[1] == :names_sep
    names_sep = esc(exprs[end].args[2]) 
    exprs = exprs[1:end-1] 
  end

  interpolated_exprs = parse_interpolation.(exprs)
  tidy_exprs = [parse_tidy(i[1]) for i in interpolated_exprs]

  df_expr = quote
      unnest_wider($(esc(df)), [$(tidy_exprs...)], names_sep=$names_sep)
  end

  return df_expr
end

function unnest_longer(df::Union{DataFrame, GroupedDataFrame}, cols; indices_include::Union{Nothing, Bool}=nothing, keep_empty::Bool=false)
  is_grouped = df isa GroupedDataFrame
  grouping_columns = is_grouped ? groupcols(df) : Symbol[]
  df_copy = copy(is_grouped ? parent(df) : df)
  
  cols_expr = cols isa Expr ? (cols,) : cols 
  column_symbols = names(df_copy, Cols(cols_expr...))

  # Handle empty arrays if keep_empty is true
    if keep_empty && keep_empty === true
        for col in column_symbols
         df_copy[!, col] = [isempty(arr) || arr === nothing ? [missing] : arr for arr in df_copy[!, col]]
        end
         flattened_df = flatten(df_copy, column_symbols, scalar=Missing)
        else
         flattened_df = flatten(df_copy, column_symbols)
    end 

    if indices_include === true
        for col in column_symbols
            col_indices = Symbol(string(col), "_id")
            indices = [j for sublist in df_copy[!, col] for j in 1:length(sublist)]
            flattened_df[!, col_indices] = indices
        end
    end

    if is_grouped
        flattened_df = groupby(flattened_df, grouping_columns)
    end

    return flattened_df
end
  
"""
$docstring_unnest_longer
"""
macro unnest_longer(df, exprs...)
    indices_include = :(nothing)  
    keep_empty = :(false)         
  
    named_args = filter(e -> isa(e, Expr) && e.head == :(=), exprs)
    for arg in named_args
        if arg.args[1] == :indices_include
            indices_include = esc(arg.args[2])
        elseif arg.args[1] == :keep_empty
            keep_empty = esc(arg.args[2])
        end
    end
    column_exprs = filter(e -> !(isa(e, Expr) && e.head == :(=)), exprs)
  
    interpolated_exprs = parse_interpolation.(column_exprs)
    tidy_exprs = [parse_tidy(i[1]) for i in interpolated_exprs]
  
    df_expr = quote
      unnest_longer($(esc(df)), [$(tidy_exprs...)], indices_include=$indices_include, keep_empty = $keep_empty)
    end
  
    return df_expr
end


function nest_by(df::DataFrame; by, key = :data)
    by_expr = by isa Expr ? (by,) : (by,)
    by_symbols = names(df, Cols(by_expr...))
  
    cols_to_nest = setdiff(names(df), by_symbols)
  
    nested_data = map(eachrow(df)) do row
        [row[c] for c in cols_to_nest]
    end
  
    nested_df = DataFrame()
    for sym in by_symbols
        nested_df[!, sym] = df[!, sym]
    end
    nested_df[!, key] = nested_data
  
    return nested_df
end
  
"""
$docstring_nest_by
"""
macro nest_by(df, args...)
    if length(args) == 2
        by_cols, new_col = args
        new_col_quoted = QuoteNode(new_col)
    elseif length(args) == 1
        by_cols = args[1]
        new_col_quoted = :(:data)  
    else
        error("Incorrect number of arguments provided to @nest")
    end
  
    interpolated_by_cols, _, _ = parse_interpolation(by_cols)
    interpolated_by_cols = parse_tidy(interpolated_by_cols)
  
    if @capture(interpolated_by_cols, (first_col:last_col))
        by_cols_expr = :($(first_col):$(last_col))
    elseif @capture(interpolated_by_cols, (args__,)) || @capture(interpolated_by_cols, [args__])
        args = QuoteNode.(args)
        by_cols_expr = :[$(args...)]
    else
        by_cols_expr = quote
            if typeof($interpolated_by_cols) <: Tuple
                collect(Symbol.($interpolated_by_cols))
            else
                $interpolated_by_cols
            end
        end
    end
  
    return quote
        nest_by($(esc(df)), by = $by_cols_expr, key = $new_col_quoted)
    end
end

function nest_pairs(df::DataFrame; kwargs...)
  result_df = copy(df)

  for (new_col_name, cols) in kwargs
      if isa(cols, Expr) && cols.head == :(:) && length(cols.args) == 2
          start_col, end_col = cols.args
          # Get index range of columns
          start_idx = findfirst(==(start_col), names(df))
          end_idx = findfirst(==(end_col), names(df))
          if isnothing(start_idx) || isnothing(end_idx)
              throw(ArgumentError("Column range $cols is invalid"))
          end
          # Convert range into a list of column names
          cols = names(df)[start_idx:end_idx]
      elseif isa(cols, Symbol)
          cols = [cols]  # Convert single column name into a list
      end

      # Get the column symbols
      column_symbols = names(df, Cols(cols))

      # Nest the specified columns into an array
      nested_column = map(eachrow(df)) do row
          [row[c] for c in column_symbols]
      end

      # Add the new nested column
      result_df[!, new_col_name] = nested_column

      # Optionally remove the original columns that were nested
       select!(result_df, Not(column_symbols))
  end

  return result_df
end

"""
$docstring_nest
"""
macro nest(df, args...)
  kwargs_exprs = []

  for arg in args
      if isa(arg, Expr) && arg.head == :(=)
          key = esc(arg.args[1])  # Extract and escape the key

          # Check if the argument is a range expression
          if isa(arg.args[2], Expr) && arg.args[2].head == :(:) && length(arg.args[2].args) == 2
              # Handle range expressions as Between selectors
              first_col, last_col = arg.args[2].args
              value_expr = Expr(:call, :Between, esc(first_col), esc(last_col))
          else
              # Apply parse_interpolation and parse_tidy to the value
              interpolated_value, _, _ = parse_interpolation(arg.args[2])
              tidy_value = parse_tidy(interpolated_value)

              # Use the existing logic for non-range expressions
              if @capture(tidy_value, (args__,)) || @capture(tidy_value, [args__])
                  args = QuoteNode.(args)
                  value_expr = :[$(args...)]
              else
                  value_expr = tidy_value
              end
          end

          # Construct the keyword argument expression
          push!(kwargs_exprs, Expr(:kw, key, value_expr))
      else
          println("Argument is not recognized as a keyword argument: ", arg)
      end
  end

  # Construct the function call to nest24 with keyword arguments
  return quote
    nest_pairs($(esc(df)), $(kwargs_exprs...))
  end
end