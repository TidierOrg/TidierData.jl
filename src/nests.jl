function unnest_wider(df::Union{DataFrame, GroupedDataFrame}, cols; names_sep::Union{String, Nothing}="_")
    is_grouped = df isa GroupedDataFrame
    grouping_columns = is_grouped ? groupcols(df) : Symbol[]
    df_copy = copy(is_grouped ? parent(df) : df)
  
    cols_expr = cols isa Expr ? (cols,) : cols
    column_symbols = names(df_copy, Cols(cols_expr...))
  
    for col in column_symbols
        col_type = typeof(df_copy[1, col])
    
        if col_type <: DataFrame
          # Handling DataFrames
          nested_col_names = unique([name for i in 1:nrow(df_copy) for name in names(df_copy[i, col])])
    
          for nested_col in nested_col_names
              new_col_name = names_sep === nothing ? nested_col : Symbol(string(col, names_sep, nested_col))
              combined_nested_col = Any[missing for _ in 1:nrow(df_copy)]
    
              for row in 1:nrow(df_copy)
                  nested_df = df_copy[row, col]
                  if ncol(nested_df) > 0 && haskey(nested_df[1, :], nested_col)
                      combined_nested_col[row] = nested_df[!, nested_col]
                      # Extract single value if there's only one element
                      if length(combined_nested_col[row]) == 1
                          combined_nested_col[row] = combined_nested_col[row][1]
                      end
                  end
              end
              df_copy[!, new_col_name] = combined_nested_col
          end
      elseif col_type <: NamedTuple || col_type <: Union{NamedTuple, Missing}
          # Handling NamedTuples and missing values
          keys_set = Set{Symbol}()
          for item in df_copy[!, col]
              if item !== missing
                  union!(keys_set, keys(item))
              end
          end
    
          for key in keys_set
              new_col_name = names_sep === nothing ? key : Symbol(string(col, names_sep, key))
              df_copy[!, new_col_name] = [item !== missing ? get(item, key, missing) : missing for item in df_copy[!, col]]
          end
      
  
        elseif col_type <: Dict
            keys_set = Set{String}()
            for item in df_copy[!, col]
                union!(keys_set, keys(item))
            end
        
            for key in keys_set
                new_col_name = names_sep === nothing ? Symbol(key) : Symbol(string(col, names_sep, key))
                df_copy[!, new_col_name] = get.(df_copy[!, col], Ref(key), missing)
            end        
  
        elseif col_type <: Array
            n = length(first(df_copy[!, col]))
            for i in 1:n
                new_col_name = names_sep === nothing ? Symbol(string(col, i)) : Symbol(string(col, names_sep, i))
                try 
                    df_copy[!, new_col_name] = getindex.(df_copy[!, col], i)
                catch
                    throw("Try using `@unnest_longer($col)` before `@unnest_wider(attribute)`")
                end
            end
        elseif col_type <: Tuple || (col_type <: Union{Tuple, Missing})
            nonmissing = filter(x -> x !== missing, df_copy[!, col])
            n = length(first(nonmissing))
            for i in 1:n
                new_col_name = names_sep === nothing ? Symbol(string(col, i)) : Symbol(string(col, names_sep, i))
                try 
                    df_copy[!, new_col_name] = getindex.(df_copy[!, col], i)
                catch
                    throw("Error unnesting tuple from column $col. Try using `@unnest_longer($col)` before `@unnest_wider(attribute)`")
                end
            end
        
        elseif any(x -> x isa Dict, df_copy[!, col])
            keys_set = Set{String}()
            for item in df_copy[!, col]
                if item isa Dict
                    union!(keys_set, keys(item))
                end
            end
            for key in keys_set
                new_col_name = names_sep === nothing ? Symbol(key) : Symbol(string(col, names_sep, key))
                df_copy[!, new_col_name] = [item isa Dict ? get(item, key, missing) : missing for item in df_copy[!, col]]
            end
        elseif any(x -> x isa Pair, df_copy[!, col])
            keys_set = Set{Any}()
            for item in df_copy[!, col]
                if item isa Pair
                    push!(keys_set, item.first)
                end
            end
            for key in keys_set
                new_col_name = names_sep === nothing ? Symbol(string(key)) : Symbol(string(col, names_sep, key))
                df_copy[!, new_col_name] = [item isa Pair && item.first == key ? item.second : missing for item in df_copy[!, col]]
            end
        else
            error("Column $col contains neither dictionaries nor arrays nor DataFrames")
        end
  
        select!(df_copy, Not(col))
    end

    if is_grouped
        df_copy = groupby(df_copy, grouping_columns)
    end
    if log[]
        @info generate_log(df, df_copy, "@unnest_wider", [:colchange])
    end
    return df_copy
end


"""
$docstring_unnest_wider
"""
macro unnest_wider(df, exprs...)
  exprs = parse_blocks(exprs...)

  names_sep = :("_") 
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

using DataFrames

function unnest_longer(df::Union{DataFrame, GroupedDataFrame}, cols; indices_include::Union{Nothing, Bool}=nothing, keep_empty::Bool=false)
    is_grouped = df isa GroupedDataFrame
    grouping_columns = is_grouped ? groupcols(df) : Symbol[]
    df_copy = copy(is_grouped ? parent(df) : df)
  
    cols_expr = cols isa Expr ? (cols,) : cols 
    column_symbols = names(df_copy, Cols(cols_expr...))
  
    # Preprocess columns
    for col in column_symbols
        df_copy[!, col] = [ismissing(x) ? (keep_empty ? [missing] : missing) :
                           isa(x, DataFrame) ? (nrow(x) > 0 ? Tables.rowtable(x) : (keep_empty ? [missing] : [])) :
                           isempty(x) ? (keep_empty ? [missing] : x) : 
                           x for x in df_copy[!, col]]
    end
  
    # Pad rows if columns have different lengths.
    for i in 1:nrow(df_copy)
        # Collect lengths of each non-missing iterable in this row
        current_lengths = [length(df_copy[i, col]) for col in column_symbols if !ismissing(df_copy[i, col])]
        if !isempty(current_lengths)
            maxlen = maximum(current_lengths)
            for col in column_symbols
                if !ismissing(df_copy[i, col])
                    arr = df_copy[i, col]
                    if length(arr) < maxlen
                        df_copy[i, col] = vcat(arr, fill(missing, maxlen - length(arr)))
                    end
                end
            end
        end
    end
  
    # Apply filter if keep_empty is false
    if !keep_empty
      df_copy = filter(row -> !any(ismissing, [row[col] for col in column_symbols]), df_copy)
    end
  
    # Flatten the dataframe
    flattened_df = flatten(df_copy, column_symbols)
  
    if indices_include === true
        for col in column_symbols
            col_indices = Symbol(string(col), "_id")
            indices = [j for i in 1:nrow(df_copy) for j in 1:length(df_copy[i, col])]
            flattened_df[!, col_indices] = indices
        end
    end
  
    if is_grouped
        flattened_df = groupby(flattened_df, grouping_columns)
    end
    if log[]
        @info  generate_log(df, flattened_df, "@unnest_longer", [:rowchange])
    end
    return flattened_df
end

  
"""
$docstring_unnest_longer
"""
macro unnest_longer(df, exprs...)
    exprs = parse_blocks(exprs...)

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


function nest_pairs(df; kwargs...)
    df_copy = copy(df)
    nested_dataframes = Dict()
    grouping_columns = names(df)
  
    # Determine grouping columns based on all specified column sets
    for (_, cols) in kwargs
        if isa(cols, Expr) && cols.head == :(:) && length(cols.args) == 2
            start_col, end_col = cols.args
            start_idx = findfirst(==(start_col), names(df))
            end_idx = findfirst(==(end_col), names(df))
            if isnothing(start_idx) || isnothing(end_idx)
                throw(ArgumentError("Column range $cols is invalid"))
            end
            cols = names(df)[start_idx:end_idx]
        elseif isa(cols, Symbol)
            cols = [cols]
        end
  
        column_symbols = names(df, Cols(cols))
        grouping_columns = setdiff(grouping_columns, column_symbols)
    end
  
    # Group the DataFrame once using these grouping columns
    grouped_df = groupby(df_copy, grouping_columns)
  
    # Nest each specified set of columns based on the single grouped DataFrame
    for (new_col_name, cols) in kwargs
        if isa(cols, Expr) && cols.head == :(:) && length(cols.args) == 2
            start_col, end_col = cols.args
            start_idx = findfirst(==(start_col), names(df))
            end_idx = findfirst(==(end_col), names(df))
            cols = names(df)[start_idx:end_idx]
        elseif isa(cols, Symbol)
            cols = [cols]
        end
  
        column_symbols = names(df, Cols(cols))
        nested_dataframes[new_col_name] = [DataFrame(select(sub_df, column_symbols)) for sub_df in grouped_df]
    end
  
    # Creating a new DataFrame with all grouping columns
    unique_groups = unique(df[:, grouping_columns])
    new_df = DataFrame(unique_groups)
  
    # Aligning and adding the nested DataFrame columns
    for (new_col_name, nested_df_list) in nested_dataframes
        aligned_nested_df = [nested_df_list[i] for i in 1:nrow(new_df)]
        new_df[!, new_col_name] = aligned_nested_df
    end
  
    return new_df
end

# For groups. Its a little bit slow i think but it works. 
# I am not sure if this is something that could ungroup -> regroup
# so for now I have opted for the safer strategy
function nest_pairs(gdf::GroupedDataFrame; kwargs...)
    group_cols = groupcols(gdf)
    results = []
    for group in gdf
        # Convert the group to a DataFrame
        df_group = DataFrame(group)
        processed_group = nest_pairs(df_group; kwargs...)
        push!(results, processed_group)
    end
    combined_df = vcat(results...)
    return groupby(combined_df, group_cols)
end


"""
$docstring_nest
"""
macro nest(df, args...)
  args = parse_blocks(args...)

  kwargs_exprs = []

  for arg in args
      if isa(arg, Expr) && arg.head == :(=)
          key = esc(arg.args[1])  # Extract and escape the key
          # this extra processing was unavoidable for some reason to enable tidy selection
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


#function nest_by(df::DataFrame; by, key = :data)
#    by_expr = by isa Expr ? (by,) : (by,)
#    by_symbols = names(df, Cols(by_expr...))
  
#    cols_to_nest = setdiff(names(df), by_symbols)
  
#    nested_data = map(eachrow(df)) do row
#        [row[c] for c in cols_to_nest]
#    end
  
#    nested_df = DataFrame()
#    for sym in by_symbols
#        nested_df[!, sym] = df[!, sym]
#    end
#    nested_df[!, key] = nested_data
#  
#    return nested_df
#end
  
#"""
#$docstring_nest_by
#"""
#macro nest_by(df, args...)
#    if length(args) == 2
#        by_cols, new_col = args
#        new_col_quoted = QuoteNode(new_col)
#    elseif length(args) == 1
#        by_cols = args[1]
#        new_col_quoted = :(:data)  
#    else
#        error("Incorrect number of arguments provided to @nest")
#    end
#  
#    interpolated_by_cols, _, _ = parse_interpolation(by_cols)
#    interpolated_by_cols = parse_tidy(interpolated_by_cols)
#  
#    if @capture(interpolated_by_cols, (first_col:last_col))
#        by_cols_expr = :($(first_col):$(last_col))
#    elseif @capture(interpolated_by_cols, (args__,)) || @capture(interpolated_by_cols, [args__])
#        args = QuoteNode.(args)
#        by_cols_expr = :[$(args...)]
#    else
#        by_cols_expr = quote
#            if typeof($interpolated_by_cols) <: Tuple
#                collect(Symbol.($interpolated_by_cols))
#            else
#                $interpolated_by_cols
#            end
#        end
#    end
#  
#    return quote
#        nest_by($(esc(df)), by = $by_cols_expr, key = $new_col_quoted)
#    end
#end