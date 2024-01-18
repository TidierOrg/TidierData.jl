function relocate(df, columns; before_column=nothing, after_column=nothing)
    cols_expr = columns isa Expr ? (columns,) : columns
    column_symbols = names(df, Cols(cols_expr...))
    column_symbols = Symbol.(column_symbols)
    # Handle before_column and after_column as collections
    before_cols = before_column isa Symbol ? [before_column] : before_column
    after_cols = after_column isa Symbol ? [after_column] : after_column
    before_col_symbols = isnothing(before_cols) ? [] : Symbol.(names(df, Cols(before_cols...)))
    after_col_symbols = isnothing(after_cols) ? [] : Symbol.(names(df, Cols(after_cols...)))
    # Convert all DataFrame column names to symbols
    df_column_names = Symbol.(names(df))
    # Reorder the columns
    new_order = Symbol[]
    inserted = false
    for col in df_column_names
        if !isempty(before_col_symbols) && col == first(before_col_symbols) && !inserted
            append!(new_order, column_symbols)  # Place all specified columns
            new_order = vcat(new_order, setdiff(before_col_symbols, column_symbols))  # Then all before columns, excluding duplicates
            inserted = true
        elseif !isempty(after_col_symbols) && col == first(after_col_symbols) && !inserted
            new_order = vcat(new_order, setdiff(after_col_symbols, column_symbols))  # Place all after columns, excluding duplicates
            append!(new_order, column_symbols)  # Then all specified columns
            inserted = true
        end
        if col ∉ column_symbols && col ∉ before_col_symbols && col ∉ after_col_symbols
            push!(new_order, col)
        end
    end
    # Move columns to the leftmost position if neither before_column nor after_column is specified
    if isempty(before_col_symbols) && isempty(after_col_symbols)
        new_order = vcat(column_symbols, filter(x -> !(x in column_symbols), df_column_names))
    end
    select!(df, new_order)
end 

"""
$docstring_relocate
"""
macro relocate(df, args...)
    before_col_expr = :nothing
    after_col_expr = :nothing
    # Extract the columns_to_move expression and keyword arguments
    col_exprs = args[1:end-1] 
    last_arg = args[end] 
    # Check if the last argument is a keyword argument
    if last_arg isa Expr && last_arg.head == :(=)
          if last_arg.args[1] == :after || last_arg.args[1] == :after_column
              after_col_expr = last_arg.args[2]
          elseif last_arg.args[1] == :before || last_arg.args[1] == :before_column
              before_col_expr = last_arg.args[2]
          else
              error("Invalid keyword argument: only 'before' or 'after' are accepted.")
          end
          col_exprs = args[1:end-1]
      else
          col_exprs = args
      end
  
      # Additional check for invalid keyword arguments in the rest of args
      for arg in col_exprs
          if arg isa Expr && arg.head == :(=) && !(arg.args[1] in [:before, :before_column, :after, :after_column])
              error("Invalid keyword argument: only 'before' or 'after' are accepted.")
          end
      end
    # Parse the column expressions
    interpolated_col_exprs = parse_interpolation.(col_exprs)
    tidy_col_exprs = [parse_tidy(i[1]) for i in interpolated_col_exprs]
    # Parse before_column and after_column
    if before_col_expr != :nothing
      interpolated_before_col = parse_interpolation(before_col_expr)
      tidy_before_col_exprs = [parse_tidy(interpolated_before_col[1])]
    else
      tidy_before_col_exprs = []
    end
    if after_col_expr != :nothing
      interpolated_after_col = parse_interpolation(after_col_expr)
      tidy_after_col_exprs = [parse_tidy(interpolated_after_col[1])]
    else
      tidy_after_col_exprs = []
    end  
    relocation_expr =
          quote
            if $(esc(df)) isa GroupedDataFrame
              local df_copy = transform($(esc(df)), ungroup = false)
              relocate(df_copy, [$(tidy_col_exprs...)], before_column=[$(tidy_before_col_exprs...)], after_column=[$(tidy_after_col_exprs...)])
              local grouped_df = groupby(parent(df_copy), groupcols($(esc(df))))
              grouped_df  
          else
              local df_copy = copy($(esc(df)))
              relocate(df_copy, [$(tidy_col_exprs...)], before_column=[$(tidy_before_col_exprs...)], after_column=[$(tidy_after_col_exprs...)])
              df_copy  
          end
      end
  
      return relocation_expr
end