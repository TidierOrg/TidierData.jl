function safe_getindex(arr, index, default_value="")
    if index <= length(arr)
        return arr[index]
    else
        return default_value
    end
end

"""
$docstring_separate
"""
macro separate(df, from, into, sep, args...)
  extra = "merge"
  for arg in args
    if isa(arg, Expr) && arg.head == :(=)
        if arg.args[1] == :extra
            extra = arg.args[2]
        end
    end
 end

  from_quoted = QuoteNode(from)
  
  interpolated_into, _, _ = parse_interpolation(into)
  
  if @capture(interpolated_into, (args__,)) || @capture(interpolated_into, [args__])
      args = QuoteNode.(args)
      into_expr = :[$(args...)]
  else
      into_expr = quote
          if typeof($interpolated_into) <: Vector{String}
              Symbol.($interpolated_into)
          else
              $interpolated_into
          end
      end
  end
  
  return quote
      separate($(esc(df)), $(from_quoted), $(into_expr), $(esc(sep)); extra=$(esc(extra)))
  end
end

function separate(df::DataFrame, col::Symbol, into::Vector{Symbol}, sep::Union{Regex, String}; extra::String = "merge")
  new_df = df[:, :]
  new_cols = map(x -> split(x, sep), new_df[:, col])
  max_cols = maximum(length.(new_cols))

  if length(into) < max_cols && extra == "warn"
      @warn "Dropping extra split parts that don't fit into the provided `into` columns."
      max_cols = length(into)
  elseif length(into) < max_cols && extra == "drop"
      max_cols = length(into)
  elseif length(into) < max_cols && extra == "merge"
      merge = true
  elseif length(into) < max_cols
      error("Not enough names provided in \"into\" for all split columns.")
  else
      merge = false
  end

  for i in 1:length(into)
      if i < length(into) || (extra == "warn" && i <= max_cols) || (extra == "drop" && i <= max_cols)
          new_df[:, into[i]] = map(x -> safe_getindex(x, i, missing), new_cols)
      elseif i == length(into) && merge
        new_df[:, into[i]] = map(x -> length(x) >= i ? join(x[i:end], sep) : missing, new_cols)
      else
          for i in 1:max_cols
              new_df[:, into[i]] = map(x -> safe_getindex(x, i, missing), new_cols)
          end
      end
  end

  new_df = select(new_df, Not(col))
  
  if log[]
    @info log_separate_changes(df, new_df, into) 
  end

  return new_df
end


"""
$docstring_unite
"""
macro unite(df, new_col, from_cols, sep, args...)
  remove=true
  for arg in args
    if isa(arg, Expr) && arg.head == :(=)
        if arg.args[1] == :remove
            remove = arg.args[2]
        end
    end
 end
  new_col_quoted = QuoteNode(new_col)
  interpolated_from_cols, _, _ = parse_interpolation(from_cols)
  interpolated_from_cols = parse_tidy(interpolated_from_cols)

  if @capture(interpolated_from_cols, (first_col:last_col))
      from_cols_expr = :($(first_col):$(last_col))
  elseif @capture(interpolated_from_cols, (args__,)) || @capture(interpolated_from_cols, [args__])
      args = QuoteNode.(args)
      from_cols_expr = :[$(args...)]
  else
      from_cols_expr = quote
          if typeof($interpolated_from_cols) <: Tuple
              collect(Symbol.($interpolated_from_cols))
          else
              $interpolated_from_cols
          end
      end
  end
  
  return quote
      unite($(esc(df)), $new_col_quoted, [$(from_cols_expr)], $(esc(sep)); remove=$(esc(remove)))
    end
end


function unite(df::DataFrame, new_col_name::Symbol, columns, sep::String="_"; remove::Bool=true)
  new_df = df[:, :]
  cols_expr = columns isa Expr ? (columns,) : columns
  column_symbols = names(df, Cols(cols_expr...)) 
  new_df[:, new_col_name] = [join(skipmissing(row), sep) for row in eachrow(df[:, column_symbols])]

  if remove
      new_df = select(new_df, Not(column_symbols))
  end
  if log[] 
    @info log_unite_changes(df, df_output, new_col_name; remove=remove, join_type="@unite")
  end
  return new_df
end


"""
$docstring_separate_rows
"""
macro separate_rows(df, exprs...)
  exprs = parse_blocks(exprs...)
  
  delimiter = esc(last(exprs)) # extract the delimiter
  exprs = Base.front(exprs) # select all but the last value
  interpolated_exprs = parse_interpolation.(exprs)

  tidy_exprs = [i[1] for i in interpolated_exprs]
  any_found_n = any([i[2] for i in interpolated_exprs])
  any_found_row_number = any([i[3] for i in interpolated_exprs])

  tidy_exprs = parse_tidy.(tidy_exprs)
  df_expr = quote
    if $any_found_n || $any_found_row_number
      if $(esc(df)) isa GroupedDataFrame
        local df_copy = transform($(esc(df)); ungroup = false)
      else
        local df_copy = copy($(esc(df)))
      end
    else
      local df_copy = $(esc(df)) # not a copy
    end
    
    if $(esc(df)) isa GroupedDataFrame
      if $any_found_n
        transform!(df_copy, nrow => :TidierData_n; ungroup = false)
      end
      if $any_found_row_number
        transform!(df_copy, eachindex => :TidierData_row_number; ungroup = false)
      end    
      
      local df_output = separate_rows(df_copy, [$(tidy_exprs...)], $delimiter)
      
      if $any_found_n || $any_found_row_number
        select!(df_output, Cols(Not(r"^(TidierData_n|TidierData_row_number)$")); ungroup = false)
      end
    else
      if $any_found_n
        transform!(df_copy, nrow => :TidierData_n)
      end
      if $any_found_row_number
        transform!(df_copy, eachindex => :TidierData_row_number)
      end
        
      local df_output = separate_rows(df_copy, [$(tidy_exprs...)], $delimiter)

      if $any_found_n || $any_found_row_number
        select!(df_output, Cols(Not(r"^(TidierData_n|TidierData_row_number)$")))
      end
    end

    df_output
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

### separate_rows
function separate_rows(df::Union{DataFrame, GroupedDataFrame}, columns, delimiter::Union{Regex, String})
  is_grouped = df isa GroupedDataFrame
  grouping_columns = is_grouped ? groupcols(df) : Symbol[]
  # Ungroup if necessary
  temp_df = copy(is_grouped ? parent(df) : df)

  cols_expr = columns isa Expr ? (columns,) : columns
  column_symbols = names(df, Cols(cols_expr...)) 
  column_symbols = Symbol.(column_symbols) 

  # Initialize an array to hold expanded data for each column
  expanded_data = Dict{Symbol, Vector{Any}}()

  for column in column_symbols
      expanded_data[column] = []

      for row in eachrow(temp_df)
          value = row[column]
          # Handle missing values and non-string types
          if ismissing(value) || !(value isa AbstractString)
            push!(expanded_data[column], [value])
          else
              push!(expanded_data[column], split(value, delimiter))
          end
      end
  end

  # Replace the columns with expanded data
  for column in column_symbols
      temp_df[!, column] = expanded_data[column]
  end

  # Flatten the DataFrame only once after all columns have been expanded
  temp_df = flatten(temp_df, column_symbols)
  if is_grouped
    temp_df = groupby(temp_df, grouping_columns)
   end

  if log[]
    @info  generate_log(df, temp_df, "@separate_rows", [:rowchange])
  end

  return temp_df
end