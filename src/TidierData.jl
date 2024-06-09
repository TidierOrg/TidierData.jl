module TidierData

using DataFrames
using MacroTools
using Chain
using Statistics
using StatsBase # primarily for `sample()`
import Cleaner # changed from `using Cleaner` because of name conflict with `DataFrames.rename()`
using Reexport

# Exporting `Cols` because `summarize(!!vars, funs))` with multiple interpolated
# columns requires `Cols()` to be nested within `Cols()`, so `Cols` needs to be exported.
@reexport using DataFrames: DataFrame, Cols, describe, nrow, proprow, Not, Between, select
@reexport using Chain
@reexport using Statistics
@reexport using ShiftedArrays: lag, lead

export TidierData_set, across, desc, n, row_number, everything, starts_with, ends_with, matches, if_else, case_when, ntile, 
      as_float, as_integer, as_string, is_number, is_float, is_integer, is_string, missing_if, replace_missing, where,
      @select, @transmute, @rename, @mutate, @summarize, @summarise, @filter,
      @group_by, @ungroup, @slice, @arrange, @distinct, @pull, @left_join, @right_join, @inner_join, @full_join, @anti_join, @semi_join,
      @pivot_wider, @pivot_longer, @bind_rows, @bind_cols, @clean_names, @count, @tally, @drop_missing, @glimpse, @separate,
      @unite, @summary, @fill_missing, @slice_sample, @slice_min, @slice_max, @slice_head, @slice_tail, @rename_with, @separate_rows,
      @unnest_longer, @unnest_wider, @nest, @relocate

# Package global variables
const code = Ref{Bool}(false) # output DataFrames.jl code?
const log = Ref{Bool}(false) # output tidylog output? (not yet implemented)

# The global do-not-vectorize "list"
const not_vectorized = Ref{Vector{Symbol}}([:getindex, :rand, :esc, :Ref, :Set, :Cols, :collect, :(:), :∘, :lag, :lead, :ntile, :repeat, :across, :desc, :mean, :std, :var, :median, :mad, :first, :last, :minimum, :maximum, :sum, :length, :skipmissing, :quantile, :passmissing, :cumsum, :cumprod, :accumulate, :is_float, :is_integer, :is_string, :cat_rev, :cat_relevel, :cat_infreq, :cat_lump, :cat_reorder, :cat_collapse, :cat_lump_min, :cat_lump_prop, :categorical, :as_categorical, :is_categorical, :unique, :iqr])

# The global do-not-escape "list"
# `in`, `∈`, and `∉` should be vectorized in auto-vec but not escaped
const not_escaped = Ref{Vector{Symbol}}([:n, :row_number, :where, :esc, :in, :∈, :∉, :Ref, :Set, :Cols, :collect, :(:), :∘, :(=>), :across, :desc, :mean, :std, :var, :median, :first, :last, :minimum, :maximum, :sum, :length, :skipmissing, :quantile, :passmissing, :startswith, :contains, :endswith])

# Includes
include("docstrings.jl")
include("parsing.jl")
include("slice.jl")
include("joins.jl")
include("binding.jl")
include("pivots.jl")
include("compound_verbs.jl")
include("clean_names.jl")
include("conditionals.jl")
include("pseudofunctions.jl")
include("helperfunctions.jl")
include("ntile.jl")
include("type_conversions.jl")
include("separate_unite.jl")
include("summary.jl")
include("is_type.jl")
include("missings.jl")
include("nests.jl")
include("relocate.jl")

# Function to set global variables
"""
$docstring_TidierData_set
"""
function TidierData_set(option::AbstractString, value::Bool)
  if option == "code"
    code[] = value
  elseif option == "log"
    throw("Logging is not enabled yet")
  else
    throw("That is not a valid option.")
  end
end

"""
$docstring_select
"""
macro select(df, exprs...)
  exprs = parse_blocks(exprs...)
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
      
      local df_output = select(df_copy, $(tidy_exprs...); ungroup = false)
      
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
        
      local df_output = select(df_copy, $(tidy_exprs...))

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

"""
$docstring_transmute
"""
macro transmute(df, exprs...)
  exprs = parse_blocks(exprs...)
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
      
      local df_output = select(df_copy, $(tidy_exprs...); ungroup = false)
      
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
        
      local df_output = select(df_copy, $(tidy_exprs...))

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

"""
$docstring_rename
"""
macro rename(df, exprs...)
  exprs = parse_blocks(exprs...)
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
      
      local df_output = rename(df_copy, $(tidy_exprs...); ungroup = false)
      
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
      
      local df_output = rename(df_copy, $(tidy_exprs...))
      
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

"""
$docstring_mutate
"""
macro mutate(df, exprs...)
  exprs = parse_blocks(exprs...)
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

      local df_output = transform(df_copy, $(tidy_exprs...); ungroup = false)

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
      
      local df_output = transform(df_copy, $(tidy_exprs...))

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

"""
$docstring_summarize
"""
macro summarize(df, exprs...)
  exprs = parse_blocks(exprs...)
  interpolated_exprs = parse_interpolation.(exprs; from_summarize = true)

  tidy_exprs = [i[1] for i in interpolated_exprs]
  any_found_n = any([i[2] for i in interpolated_exprs])
  any_found_row_number = any([i[3] for i in interpolated_exprs])

  tidy_exprs = parse_tidy.(tidy_exprs; autovec=true) # use auto-vectorization inside `@summarize()`
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
      local col_names = groupcols($(esc(df)))
      if $any_found_n
        transform!(df_copy, nrow => :TidierData_n; ungroup = false)  
      end
      if $any_found_row_number
        transform!(df_copy, eachindex => :TidierData_row_number; ungroup = false)  
      end
      
      if length(col_names) == 1
        local df_output = combine(df_copy, $(tidy_exprs...); ungroup = true)
        if $any_found_n || $any_found_row_number
          select!(df_output, Cols(Not(r"^(TidierData_n|TidierData_row_number)$")))
        end
      else
        local df_output = combine(df_copy, $(tidy_exprs...); ungroup = true)
        if $any_found_n || $any_found_row_number
          select!(df_output, Cols(Not(r"^(TidierData_n|TidierData_row_number)$")))
        end
        df_output = groupby(df_output, col_names[1:end-1]; sort = false)
      end
    else
      if $any_found_n
        transform!(df_copy, nrow => :TidierData_n; ungroup = false)
      end
      if $any_found_row_number
        transform!(df_copy, eachindex => :TidierData_row_number; ungroup = false)  
      end
      local df_output = combine(df_copy, $(tidy_exprs...))
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

"""
$docstring_summarize
"""
macro summarise(df, exprs...)
  :(@summarize($(esc(df)), $(exprs...)))
end

"""
$docstring_filter
"""
macro filter(df, exprs...)
  exprs = parse_blocks(exprs...)
  interpolated_exprs = parse_interpolation.(exprs)

  tidy_exprs = [i[1] for i in interpolated_exprs]
  any_found_n = any([i[2] for i in interpolated_exprs])
  any_found_row_number = any([i[3] for i in interpolated_exprs])

  tidy_exprs = parse_tidy.(tidy_exprs; subset=true)
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

      local df_output = subset(df_copy, $(tidy_exprs...); skipmissing = true, ungroup = false)
      
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

      local df_output = subset(df_copy, $(tidy_exprs...); skipmissing = true)

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

"""
$docstring_group_by
"""
macro group_by(df, exprs...)
  exprs = parse_blocks(exprs...)
  interpolated_exprs = parse_interpolation.(exprs)

  tidy_exprs = [i[1] for i in interpolated_exprs]
  any_found_n = any([i[2] for i in interpolated_exprs])
  any_found_row_number = any([i[3] for i in interpolated_exprs])

  tidy_exprs = parse_tidy.(tidy_exprs)
  grouping_exprs = parse_group_by.(exprs)
  grouping_exprs = parse_tidy.(grouping_exprs)

  df_expr = quote
    local any_expressions = any(typeof.($tidy_exprs) .!= QuoteNode)

    if $any_found_n || $any_found_row_number || any_expressions
      if $(esc(df)) isa GroupedDataFrame
        local df_copy = transform($(esc(df)); ungroup = false)
      else
        local df_copy = copy($(esc(df)))
      end
    else
      local df_copy = $(esc(df)) # not a copy
    end

    if $any_found_n
      transform!(df_copy, nrow => :TidierData_n)
    end
    if $any_found_row_number
      transform!(df_copy, eachindex => :TidierData_row_number)
    end
    
    if any_expressions
      transform!(df_copy, $(tidy_exprs...))
    end

    if $any_found_n || $any_found_row_number
      select!(df_copy, Cols(Not(r"^(TidierData_n|TidierData_row_number)$")))
    end
    
    # removed ;sort = true for speed reasons
    # this can cause a large number of allocations when the grouped variable is a string
    local df_output = groupby(df_copy, Cols($(grouping_exprs...)); sort = false)

    df_output
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

"""
$docstring_ungroup
"""
macro ungroup(df)
  df_expr = quote 
    if $(esc(df)) isa GroupedDataFrame
      transform($(esc(df)); ungroup = true)
    else
      copy($(esc(df)))
    end
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

"""
$docstring_arrange
"""
macro arrange(df, exprs...)
  exprs = parse_blocks(exprs...)
  arrange_exprs = parse_desc.(exprs)
  df_expr = quote
    if $(esc(df)) isa GroupedDataFrame
      local col_names = groupcols($(esc(df)))
      
      @chain $(esc(df)) begin
        DataFrame # remove grouping
        sort([$(arrange_exprs...)]) # Must use [] instead of Cols() here
        groupby(col_names; sort = false) # regroup
      end
    else
      sort($(esc(df)), [$(arrange_exprs...)]) # Must use [] instead of Cols() here
    end
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

"""
$docstring_distinct
"""
macro distinct(df, exprs...)
  exprs = parse_blocks(exprs...)
  interpolated_exprs = parse_interpolation.(exprs)

  tidy_exprs = [i[1] for i in interpolated_exprs]
  any_found_n = any([i[2] for i in interpolated_exprs])
  any_found_row_number = any([i[3] for i in interpolated_exprs])

  tidy_exprs = parse_tidy.(tidy_exprs)
  df_expr = quote
    if $(esc(df)) isa GroupedDataFrame
      local col_names = groupcols($(esc(df)))

      # `@distinct()` uses a different pattern from the other macros
      # because if the original DataFrame is grouped, it must be ungrouped
      # and then regrouped, so there's no need to make a copy up front.
      # This is because `unique()` does not work on GroupDataFrames.
      local df_copy = transform($(esc(df)); ungroup = true)
      if $any_found_n
        transform!(df_copy, nrow => :TidierData_n)
      end
      if $any_found_row_number
        transform!(df_copy, eachindex => :TidierData_row_number)
      end
      if length([$tidy_exprs...]) == 0
        unique!(df_copy)
      else
        unique!(df_copy, Cols($(tidy_exprs...)))
      end

      if $any_found_n || $any_found_row_number
        select!(df_copy, Cols(Not(r"^(TidierData_n|TidierData_row_number)$")))
      end
      groupby(df_copy, col_names; sort = false) # regroup and value to return
    else
      local df_copy = copy($(esc(df)))
      if $any_found_n
        transform!(df_copy, nrow => :TidierData_n)
      end
      if $any_found_row_number
        transform!(df_copy, eachindex => :TidierData_row_number)
      end
      if length([$tidy_exprs...]) == 0
        unique!(df_copy)
      else
        unique!(df_copy, Cols($(tidy_exprs...)))
      end
      
      if $any_found_n || $any_found_row_number
        select!(df_copy, Cols(Not(r"^(TidierData_n|TidierData_row_number)$")))
      end

      df_copy # value to return
    end
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

"""
$docstring_pull
"""
macro pull(df, column)
  column, found_n, found_row_number = parse_interpolation(column)
  column = parse_tidy(column)
  vec_expr = quote
    $(esc(df))[:, $column]
  end
  if code[]
    @info MacroTools.prettify(vec_expr)
  end
  return vec_expr
end

"""
$docstring_glimpse
"""
macro glimpse(df, width = 80)
  df_expr = quote
    # DataFrame() needed to handle grouped data frames
    println("Rows: ", nrow(DataFrame($(esc(df)))))
    println("Columns: ", ncol(DataFrame($(esc(df)))))

    if $(esc(df)) isa GroupedDataFrame
      println("Groups: ", join(string.(groupcols($(esc(df)))), ", "), " [", length(keys($(esc(df)))), "]")
    end

    for (name, col) in pairs(eachcol(DataFrame($(esc(df)))))
      rpad("." * string(name), 15) *
      rpad(eltype(col), 15) *
      join(col, ", ") |>
      x -> first(x, $width) |> # show the first $width number of characters
      println
    end
  end 
  return df_expr
end

"""
$docstring_rename_with
"""
macro rename_with(df, fn, exprs...)
  exprs = parse_blocks(exprs...)
  interpolated_exprs = parse_interpolation.(exprs)

  tidy_exprs = [i[1] for i in interpolated_exprs]
  any_found_n = any([i[2] for i in interpolated_exprs])
  any_found_row_number = any([i[3] for i in interpolated_exprs])

  tidy_exprs = parse_tidy.(tidy_exprs)
  df_expr = quote
      local df_copy = copy($(esc(df)))

      if $any_found_n
          if df_copy isa GroupedDataFrame
              transform!(df_copy, nrow => :TidierData_n; ungroup = false)
          else
              transform!(df_copy, nrow => :TidierData_n)
          end
      end
      
      if $any_found_row_number
          if df_copy isa GroupedDataFrame
              transform!(df_copy, eachindex => :TidierData_row_number; ungroup = false)
          else
              transform!(df_copy, eachindex => :TidierData_row_number)
          end
      end

      local columns_to_rename
      if isempty($(esc(exprs)))
          columns_to_rename = names(df_copy)
      else
          columns_to_rename = names(select(copy(df_copy), $(tidy_exprs...)))
      end

      local renames = filter(p -> p.first in columns_to_rename, Pair.(names(df_copy), $(esc(fn)).(names(df_copy))))

      if df_copy isa GroupedDataFrame
          rename!(df_copy, renames; ungroup = false)
      else
          rename!(df_copy, renames)
      end

      df_copy
  end

  return df_expr
end

end