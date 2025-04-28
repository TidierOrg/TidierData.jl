"""
$docstring_pivot_wider
"""
macro pivot_wider(df, exprs...)
    exprs = parse_blocks(exprs...)
    # take the expressions and return arg => value dictionary    
    interpolated_exprs = parse_interpolation.(exprs)

    tidy_exprs = [i[1] for i in interpolated_exprs]
    # commented out because not needed here
    # any_found_n = any([i[2] for i in interpolated_exprs])
    # any_found_row_number = any([i[3] for i in interpolated_exprs])

    tidy_exprs = parse_pivot_arg.(tidy_exprs)
    expr_dict = Dict(x.args[2] => x.args[3] for x in tidy_exprs)

    # we need to define a dictionary 
    # to hold arguments in the format expected by unstack()
    arg_dict = Dict{Symbol, Any}()

    if haskey(expr_dict, QuoteNode(:values_fill))
        arg_dict[:fill] = eval(expr_dict[QuoteNode(:values_fill)])
    end

    names_from  = expr_dict[QuoteNode(:names_from)]
    values_from = expr_dict[QuoteNode(:values_from)]

    return quote
        vals = $(values_from)
        if vals isa Vector
            pivot_wider_multi($(esc(df)),
                               $(names_from),
                               vals;
                               $(arg_dict)...)
        else
            unstack(($(esc(df))),
                    $(names_from),
                    vals;
                    $(arg_dict)...)
        end
    end
end

function pivot_wider_multi(df::AbstractDataFrame,
                            names_from_raw,
                            values_from::Vector{Symbol};
                            fill = missing)

    # resolve column references exactly as stored in `df`
    raw_name  = names_from_raw isa QuoteNode ? names_from_raw.value : names_from_raw
    name_col  = first(col for col in names(df) if String(col) == String(raw_name))

    val_cols  = [first(col for col in names(df) if String(col) == String(v))
                 for v in values_from]

    id_cols   = setdiff(names(df), vcat([name_col], val_cols))

    result = nothing

    for (i, v) in enumerate(val_cols)
        sel_cols = vcat(id_cols, [name_col, v]) |> unique
        tmp      = df[:, sel_cols]                             # copy for this pass

        wide     = unstack(tmp, name_col, v; fill = fill)

        if name_col in names(wide)                             # drop names_from col
            select!(wide, Not(name_col))
        end

        suffix   = String(values_from[i])                      # original col symbol
        rename!(wide, Dict(c => Symbol(string(c), "_", suffix)
                     for c in setdiff(names(wide), id_cols)))

        if result === nothing
            result = wide
        else
            sort!(wide, id_cols)                               # align rows before hcat
            result = hcat(result, select(wide, Not(id_cols)); makeunique = true)
        end
    end
    return result
end

"""
$docstring_pivot_longer
"""
macro pivot_longer(df, exprs...)
    if length(exprs) == 0
        exprs = (:(everything()),)
    end
    exprs = parse_blocks(exprs...)
    
    # take the expressions and return arg => value dictionary 
    interpolated_exprs = parse_interpolation.(exprs)

    tidy_exprs = [i[1] for i in interpolated_exprs]
    # commented out because not needed here
    # any_found_n = any([i[2] for i in interpolated_exprs])
    # any_found_row_number = any([i[3] for i in interpolated_exprs])

    tidy_exprs = parse_pivot_arg.(tidy_exprs)
    expr_dict = Dict(x.args[2] => x.args[3] for x in tidy_exprs)

    # we need to define a dictionary 
    # to hold arguments in the format expected by stack()
    arg_dict = Dict{Symbol, Any}()

    # if names_to was specified, pass that argument to variable_name
    if haskey(expr_dict, QuoteNode(:names_to))
        arg_dict[:variable_name] = (expr_dict[QuoteNode(:names_to)]).value
    end

    # if values_to was specified, pass that argument to value_name
    if haskey(expr_dict, QuoteNode(:values_to))
        arg_dict[:value_name] = (expr_dict[QuoteNode(:values_to)]).value
    end

    # splat any specified arguments in to stack()
    df_expr = quote
        stack(DataFrame($(esc(df))), $(expr_dict[QuoteNode(:cols)]); $(arg_dict)...)
    end

    if code[]
        @info MacroTools.prettify(df_expr)
    end
    
    return df_expr
end

