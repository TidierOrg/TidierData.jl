# Not exported
function parse_tidy(tidy_expr::Union{Expr,Symbol,Number, QuoteNode}; # Can be symbol or expression
                    autovec::Bool=true, subset::Bool=false, from_across::Bool=false,
                    from_slice::Bool = false)
  if @capture(tidy_expr, across(vars_, funcs_))
    return parse_across(vars, funcs)
  elseif from_slice && @capture(tidy_expr, -var_)
    return :($var), true # true = negated
  elseif from_slice && @capture(tidy_expr, var_Number)
    if var > 0
      return tidy_expr, false # false = not negated
    elseif var < 0
      return -tidy_expr, true # true = negated
    else
      throw("Numeric selections cannot be zero.")
    end
  elseif from_slice
    return tidy_expr, false
  elseif @capture(tidy_expr, -(startindex_:endindex_) | !(startindex_:endindex_))
    if startindex isa Symbol
      startindex = QuoteNode(startindex)
    end
    if endindex isa Symbol
      endindex = QuoteNode(endindex)
    end
    return :(Not(Between($startindex, $endindex)))
  elseif @capture(tidy_expr, startindex_:endindex_)
    if startindex isa Symbol
      startindex = QuoteNode(startindex)
    end
    if endindex isa Symbol
      endindex = QuoteNode(endindex)
    end
    return :(Between($startindex, $endindex))
  elseif @capture(tidy_expr, (lhs_ = fn_(args__)) | (lhs_ = fn_.(args__)))
    if length(args) == 0
      lhs = QuoteNode(lhs)
      return :($fn => $lhs)
    else
      @capture(tidy_expr, lhs_ = rhs_)
      return parse_function(lhs, rhs; autovec, subset)
    end
  elseif @capture(tidy_expr, lhs_ = rhs_)
    if rhs isa Symbol
      lhs = QuoteNode(lhs)
      rhs = QuoteNode(rhs)
      return :($rhs => $lhs)
    else # handles @mutate(b = 10)
      return parse_function(lhs, :(identity($rhs)); autovec, subset)
    end
  elseif @capture(tidy_expr, -var_Symbol)
    var = QuoteNode(var)
    return :(Not($var))
  elseif @capture(tidy_expr, !var_Symbol)
    var = QuoteNode(var)
    return :(Not($var))
  elseif @capture(tidy_expr, var_Symbol)
    if var == Symbol(":")
      return var
    else
      return QuoteNode(var)
    end
  elseif @capture(tidy_expr, var_Number)
    if var > 0
      return var
    elseif var < 0
      var = -var
      return :(Not($var))
    else
      throw("Numeric selections cannot be zero.")
    end
  elseif @capture(tidy_expr, !var_Number)
    return :(Not($var))
  elseif @capture(tidy_expr, (tuple__,))
    tuple = parse_tidy.(tuple)
    return :(Cols($(tuple...)))
  elseif @capture(tidy_expr, [vec__])
    vec = parse_tidy.(vec)
    return :(Cols($(vec...)))
  elseif @capture(tidy_expr, -[vec__])
    vec = parse_tidy.(vec)
    return :(Not(Cols($(vec...)))) # can simpify to Not($(tuple...)) in DataFrames 1.6+
  elseif @capture(tidy_expr, ![vec__])
    vec = parse_tidy.(vec)
    return :(Not(Cols($(vec...)))) # can simpify to Not($(tuple...)) in DataFrames 1.6+
  elseif !subset & @capture(tidy_expr, -fn_(args__)) # negated selection helpers
    return :(Cols(!($(esc(fn))($(args...))))) # change the `-` to a `!` and return
  elseif !subset & @capture(tidy_expr, fn_(args__)) # selection helpers
    if from_across || fn == :Cols # fn == :Cols is to deal with interpolated columns
      return tidy_expr
    elseif fn == :where
      return :(Cols(all.(broadcast($(esc(args...)), eachcol(DataFrame(df_copy))))))
    elseif fn == :- || fn == :! # for negated selection as in -(A, B), which is internally represnted as function
      args = parse_tidy.(args)
      return :(Not(Cols($(args...)))) # can simpify to Not($(tuple...)) in DataFrames 1.6+
    else
      return :(Cols($(esc(tidy_expr))))
    end
  elseif subset
    return parse_function(:ignore, tidy_expr; autovec, subset)
  else
    return tidy_expr
    # return :($(esc(tidy_expr)))
    # Do not throw error because multiple functions within across() where some are anonymous require this condition
    # throw("Expression not recognized by parse_tidy()")
  end
end

# Not exported
function parse_pivot_arg(tidy_expr::Union{Expr,Symbol,Number})
  if @capture(tidy_expr, lhs_ = rhs_Symbol)
    lhs = QuoteNode(lhs)
    rhs = QuoteNode(rhs)
    return :($lhs => $rhs)
  elseif @capture(tidy_expr, lhs_ = rhs_String)
      lhs = QuoteNode(lhs)
      rhs = QuoteNode(rhs)
      return :($lhs => $rhs)
      
  # Need to avoid QuoteNode-ing rhs when rhs is an expression.
  # You can't use !! interpolation inside of for-loops because
  # macros are expanded at parse-time, so you instead need to do
  # Main.eval(:globalvar) or @eval(Main, globalvar) where globalvar
  # is assigned to equal the iterator instead of using !!globalvar,
  # which gets expanded before the for-loop is run.
  elseif @capture(tidy_expr, lhs_ = rhs_)
    lhs = QuoteNode(lhs)
    return :($lhs => $rhs)

  else
    tidy_expr = parse_tidy(tidy_expr)
    return :(:cols => $(tidy_expr))
  end
end

# Not exported
function parse_function(lhs::Union{Symbol, Expr}, rhs::Expr; autovec::Bool=true, subset::Bool=false)

  lhs = QuoteNode(lhs)

  src = Symbol[]
  MacroTools.postwalk(rhs) do x
    if @capture(x, (fn_(args__)) | (fn_.(args__))) && fn != :esc
      args = args[isa.(args, Symbol)]
      push!(src, args...)
    end
    return x
  end

  src = unique(src)
  func_left = :($(src...),)

  if autovec
    rhs = parse_autovec(rhs)
  end

  rhs = parse_escape_function(rhs) # ensure that functions in user space are available

  if subset
    return :($src => ($func_left -> $rhs)) # to ensure that missings are replace by false
  else
    return :($src => ($func_left -> $rhs) => $lhs)
  end
end

# Not exported
# Note: `parse_across` currently does not support the use of numbers for selecting columns
function parse_across(vars::Union{Expr,Symbol}, funcs::Union{Expr,Symbol})

  src = Union{Expr,QuoteNode}[] # type can be either a QuoteNode or a expression containing a selection helper function

  if @capture(vars, (args__,))
    for arg in args
        push!(src, parse_tidy(arg))
    end
  else
      push!(src, parse_tidy(vars)) 
  end

  func_array = Union{Expr,Symbol}[] # expression containing functions

  if funcs isa Symbol
    push!(func_array, esc(funcs)) # fixes bug where single function is used inside across
  elseif @capture(funcs, (args__,))
    for arg in args
      if arg isa Symbol
        push!(func_array, esc(arg))
      else
        push!(func_array, esc(parse_tidy(arg; from_across=true))) # fixes bug with compound and anonymous functions getting wrapped in Cols()
      end
    end
  else # for compound functions like mean or anonymous functions
    push!(func_array, esc(funcs))
  end

  num_funcs = length(func_array)

  return :(Cols($(src...)) .=> reshape([$(func_array...)], 1, $num_funcs))
end

# Not exported
function parse_desc(tidy_expr::Union{Expr,Symbol})
  tidy_expr, found_n, found_row_number = parse_interpolation(tidy_expr)
  if @capture(tidy_expr, Cols(args__)) # from parse_interpolation
    return :(Cols($(args...),))
  elseif @capture(tidy_expr, desc(var_))
    var = QuoteNode(var)
    return :(order($var, rev=true))
  else
    return QuoteNode(tidy_expr)
  end
end

# Not exported
function parse_join_by(tidy_expr::Union{Expr,Symbol,String})
  tidy_expr, found_n, found_row_number = parse_interpolation(tidy_expr)
  
  src = Union{Expr,QuoteNode}[] # type can be either a QuoteNode or a expression containing a selection helper function

  if @capture(tidy_expr, expr_Symbol)
    push!(src, QuoteNode(expr))
  elseif @capture(tidy_expr, expr_String)
    push!(src, QuoteNode(Symbol(expr)))
  elseif @capture(tidy_expr, lhs_Symbol = rhs_Symbol)
    lhs = QuoteNode(lhs)
    rhs = QuoteNode(rhs)
    push!(src, :($lhs => $rhs))
  elseif @capture(tidy_expr, lhs_String = rhs_String)
    lhs = QuoteNode(Symbol(lhs))
    rhs = QuoteNode(Symbol(rhs))
    push!(src, :($lhs => $rhs))
  else
    @capture(tidy_expr, (args__,))
    for arg in args
      if @capture(arg, expr_Symbol)
        push!(src, QuoteNode(expr))
      elseif @capture(arg, expr_String)
        push!(src, QuoteNode(Symbol(expr)))
      elseif @capture(arg, lhs_Symbol = rhs_Symbol)
        lhs = QuoteNode(lhs)
        rhs = QuoteNode(rhs)
        push!(src, :($lhs => $rhs))
      elseif @capture(arg, lhs_String = rhs_String)
        lhs = QuoteNode(Symbol(lhs))
        rhs = QuoteNode(Symbol(rhs))
        push!(src, :($lhs => $rhs))
      else
        push!(src, QuoteNode(arg))
      end
    end
  end
 
  return :([$(src...)]) 
end

# Not exported
function parse_group_by(tidy_expr::Union{Expr,Symbol})
  tidy_expr, found_n, found_row_number = parse_interpolation(tidy_expr)

  if @capture(tidy_expr, Cols(args__)) # from parse_interpolation
    return :(Cols($(args...),))
  elseif @capture(tidy_expr, lhs_ = rhs_)
    return QuoteNode(lhs)
  elseif tidy_expr isa Expr
    return tidy_expr
  else # if it's a Symbol
    return QuoteNode(tidy_expr)
  end
end

# Not exported
function parse_autovec(tidy_expr::Union{Expr,Symbol})

  # Use postwalk so that we capture smallest expressions first.
  # In the future, may want to consider switching to prewalk() so that we can 
  # capture the largest expression first and functions haven't already been vectorized first.
  # Because prewalk() can result in infinite loops, would require lots of careful testing.
  autovec_expr = MacroTools.postwalk(tidy_expr) do x

    # don't vectorize if starts with ~ (compound function)
    # The reason we have a . is that bc this is postwalk, the function will first have been 
    # vectorized, and we need to unvectorize it.
    # Adding the non-vectorized condition in case a non-vectorized function like mean is accidentally
    # prefixed with a ~.
    if @capture(x, (~fn1_ ∘ fn2_.(args__)) | (~fn1_ ∘ fn2_(args__)))
      return :($fn1 ∘ $fn2($(args...)))

      # Don't vectorize if starts with ~ (regular function)
      # The reason we have a . is that bc this is postwalk, the function will first have been 
      # vectorized, and we need to unvectorize it.
      # Adding the non-vectorized condition in case a non-vectorized function like mean is accidentally
      # prefixed with a ~.
    elseif @capture(x, (~fn_.(args__)) | (~fn_(args__)))
      return :($fn($(args...)))

      # Don't vectorize if starts with ~ (operator) e.g., a ~+ b
    elseif @capture(x, args1_ ~ fn_(args2_))
      # We need to remove the . from the start bc was already vectorized and we need to 
      # unvectorize it
      fn_new = Symbol(string(fn)[2:end])
      return :($fn_new($args1, $args2))
    
    # If user already does Ref(Set(arg2)), then vectorize and leave as-is
    elseif @capture(x, in(arg1_, Ref(Set(arg2_))))
        return :(in.($arg1, Ref(Set($arg2))))
    
    # If user already does Ref(arg2), then wrap arg2 inside of a Set().
    # Set requires additional allocation but is much faster.
    # See: https://bkamins.github.io/julialang/2023/02/10/in.html
    elseif @capture(x, in(arg1_, Ref(arg2_)))
      return :(in.($arg1, Ref(Set($arg2))))
    
    # If user already does Set(arg2), then wrap this inside of Ref().
    # This is required to prevent vectorization of arg2.
    elseif @capture(x, in(arg1_, Set(arg2_))) 
      return :(in.($arg1, Ref(Set($arg2))))
    
    # If user did provides bare vector or tuple for arg2, then wrap
    # arg2 inside of a Ref(Set(arg2))
    # This is required to prevent vectorization of arg2.
    elseif @capture(x, in(arg1_, arg2_))
      return :(in.($arg1, Ref(Set($arg2))))

    # Handle ∈
    elseif @capture(x, ∈(arg1_, Ref(Set(arg2_))))
      return :((∈).($arg1, Ref(Set($arg2))))
    elseif @capture(x, ∈(arg1_, Ref(arg2_)))
      return :((∈).($arg1, Ref(Set($arg2))))
    elseif @capture(x, ∈(arg1_, Set(arg2_)))
      return :((∈).($arg1, Ref(Set($arg2))))
    elseif @capture(x, ∈(arg1_, arg2_))
      return :((∈).($arg1, Ref(Set($arg2))))

  # Handle ∉
    elseif @capture(x, ∉(arg1_, Ref(Set(arg2_))))
      return :((∉).($arg1, Ref(Set($arg2))))
    elseif @capture(x, ∉(arg1_, Ref(arg2_)))
      return :((∉).($arg1, Ref(Set($arg2))))
    elseif @capture(x, ∉(arg1_, Set(arg2_)))
      return :((∉).($arg1, Ref(Set($arg2))))
    elseif @capture(x, ∉(arg1_, arg2_))
      return :((∉).($arg1, Ref(Set($arg2))))

    elseif @capture(x, fn_(args__))
      # This is the do-not-vectorize "list"
      # `in` should be vectorized so do not add to this exclusion list
      if fn in not_vectorized[]
        return x
      elseif contains(string(fn), r"[^\W0-9]\w*$") # valid variable name
        return :($fn.($(args...)))
      elseif startswith(string(fn), ".") # already vectorized operator
        return x
      else # operator
        fn_new = Symbol("." * string(fn))
        return :($fn_new($(args...)))
      end
    elseif hasproperty(x, :head) && (x.head == :&& || x.head == :||)
      x.head = Symbol("." * string(x.head))
      return x
    end
    return x
  end
  return autovec_expr
end

# Not exported
function parse_escape_function(rhs_expr::Union{Expr,Symbol})
  rhs_expr = MacroTools.postwalk(rhs_expr) do x

    # If it's already escaped, make sure it needs to remain escaped
    if @capture(x, esc(variable_Symbol))
      if hasproperty(Base, variable) && !(typeof(getproperty(Base, variable)) <: Function)
        # Remove the escaping if referring to a constant value like Base.pi and Base.Int64
       return variable
      elseif hasproperty(Core, variable) && !(typeof(getproperty(Core, variable)) <: Function)
        # Remove the escaping if referring to a data type like Core.Int64
       return variable
      elseif hasproperty(Statistics, variable) && !(typeof(getproperty(Statistics, variable)) <: Function)
        # Because Statistics is re-exported
       return variable
      elseif variable in not_escaped[]
        return variable
      elseif contains(string(variable), r"[^\W0-9]\w*$") # valid variable name
        return esc(variable)
      else
       return variable
      end
    elseif @capture(x, fn_(args__))
      if fn in not_escaped[]
        return x
      elseif fn isa Symbol && hasproperty(Base, fn) && typeof(getproperty(Base, fn)) <: Function
        return x
      elseif fn isa Symbol && hasproperty(Core, fn) && typeof(getproperty(Core, fn)) <: Function
        return x
      elseif fn isa Symbol && hasproperty(Statistics, fn) && typeof(getproperty(Statistics, fn)) <: Function
        return x
      elseif contains(string(fn), r"[^\W0-9]\w*$") # valid variable name
        return :($(esc(fn))($(args...)))
      else
        return x
      end
    elseif @capture(x, fn_.(args__))
      if fn in not_escaped[]
        return x
      elseif fn isa Symbol && hasproperty(Base, fn) && typeof(getproperty(Base, fn)) <: Function
        return x
      elseif fn isa Symbol && hasproperty(Core, fn) && typeof(getproperty(Core, fn)) <: Function
        return x
      elseif fn isa Symbol && hasproperty(Statistics, fn) && typeof(getproperty(Statistics, fn)) <: Function
        return x
      elseif contains(string(fn), r"[^\W0-9]\w*$") # valid variable name
        return :($(esc(fn)).($(args...)))
      else
        return x
      end
    elseif @capture(x, @mac_(args__))
      if endswith(string(mac), "_str")
        # Macros used inside of string macros are escaped, making it possible to work with Unitful units inside of `@mutate` (e.g. `u"psi"`)
        return esc(Expr(:macrocall, mac, LineNumberNode, args...))
      else
        # Other macros that may reference variables referring to column names should *not* be escaped
        return x
      end
    end
    return x
  end
  return rhs_expr
end

# Not exported
# String is for parse_join_by
function parse_interpolation(var_expr::Union{Expr,Symbol,Number,String};
  from_summarize::Bool = false, from_slice::Bool = false)
  found_n = false
  found_row_number = false

  var_expr = MacroTools.postwalk(var_expr) do x
    if @capture(x, !!variable_Symbol)
      return esc(variable)
    # If a variable has already been escaped and marked with a `!!` (e.g., `!!pi`),
    # then it won't be re-escaped.
    elseif @capture(x, !!expr_)
      return expr
    # `hello` in Julia is converted to Core.@cmd("hello")
    # Since MacroTools is unable to match this pattern, we can directly
    # evaluate the expression to see if it matches. If it does, the 3rd argument
    # contains the string containing the values inside the backticks.
    elseif hasproperty(x, :head) && x.head == :macrocall &&
           hasproperty(x.args[1], :mod) && hasproperty(x.args[1], :name) &&
           x.args[1].mod == Core && x.args[1].name == Symbol("@cmd")
      return Symbol(x.args[3])
    elseif @capture(x, fn_())
      if fn == :n
        if from_summarize
          return :(nrow())
        elseif from_slice
          return :end
        else
          found_n = true # do not move this -- this leads to creation of new column
          return :(getindex(TidierData_n, 1))
        end
      elseif fn == :row_number
        found_row_number = true
        return :TidierData_row_number
      else
        return :($fn())
      end
    elseif @capture(x, esc(variable_))
      return esc(variable)
    # Escape any native Julia symbols that come from the Base or Core packages
    # This includes :missing but also includes all data types (e.g., :Real, :String, etc.)
    # To refer to a column named String, you can use `String` (in backticks)
    elseif @capture(x, variable_Symbol)
      if variable in not_escaped[]
        return variable
      elseif hasproperty(Base, variable) && !(typeof(getproperty(Base, variable)) <: Function) && !(typeof(getproperty(Base, variable)) <: Type)
        return esc(variable)
      elseif hasproperty(Core, variable) && !(typeof(getproperty(Core, variable)) <: Function) && !(typeof(getproperty(Core, variable)) <: Type)
        return esc(variable)
      elseif hasproperty(Statistics, variable) && !(typeof(getproperty(Statistics, variable)) <: Function) && !(typeof(getproperty(Statistics, variable)) <: Type)
        return esc(variable)
      else
        return variable
      end
    end
    return x
  end
  return var_expr, found_n, found_row_number
end

# Not export
# parse DataFrame and Expr
function parse_bind_args(tidy_expr::Union{Expr,Symbol})
  found_id = false
  if @capture(tidy_expr, lhs_ = rhs_)
    if lhs != :id
      throw("$(String(lhs)) is not implemented")
    else
      found_id = true
      return rhs, found_id
    end
  end
  return esc(tidy_expr), found_id
end

# Not export
# converts begin-end blocks into a tuple of expressions
function parse_blocks(exprs...)
  if length(exprs) == 1 && hasproperty(exprs[1], :head) && exprs[1].head == :block
    return (MacroTools.rmlines(exprs[1]).args...,)
  end
  return exprs
end
