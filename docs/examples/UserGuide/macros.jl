# While TidierData relies heavily on macros, you may occasionally find yourself needing to use other macros *within* TidierData macros. This can result in errors that may be hard to interpret. This page is intended to demonstrate two common situations when working with macros: string macros and macros with columns as arguments.

# ## String macros

# You may not think of string macros as macros because we often work with them in the form of prefixes (or suffixes) attached to string literals, such as `prefix"string goes here"`. However, string macros are indeed macros, and thankfully, these should work without any modification.

using TidierData

# Let's add the `psi` unit to the `a` column using the Unitful package.

using Unitful
df = DataFrame(a = 1:5, b = 6:10)
@chain df @mutate(a = a * u"psi")

# It just works!

# ## Macros with columns as arguments

# Occasionally, you may want to work with macros that operate on columns of a data frame. You may want to apply syntax that looks like this:

# ```julia
# using Printf
# df = DataFrame(a = [0.11, 0.21, 0.12, 0.22])
# 
# @chain df begin
#     @mutate(a_label = @sprintf("Var = %.1f", a))
# end
# ```

# However, this will not work! Why not? Well, there are a two reasons: it is difficult to escape a macro but not its arguments, and macros cannot be vectorized by adding a period to the end (unlike functions).

# The easiest way to fix both issues is to wrap the macro inside of an anonymous function. Thus, `@example_macro(a)` turns into `(x -> example_macro(x))(a)`. What is happening here is that an anonymous function is being defined, and then that function is immediately being called with an argument `a` referring to the column name `a`. 

# Here is what the looks like for `@sprintf`:

using Printf
df = DataFrame(a = [0.11, 0.21, 0.12, 0.22])

@chain df begin
    @mutate(a_label = (x -> @sprintf("Var = %.1f", x))(a))
end

# This works!

# Even though TidierData cannot dot-vectorize `@sprintf`, it can vectorize the anonymous function in which `@sprintf` is wrapped, converting the expression to `a_label = (x -> @sprintf("Var = %.1f", x)).(a)` before it is run. Notice that TidierData adds a period before the `(a)` to vectorize the function before passing this expression to DataFrames.jl.

# Lastly, one caveat to know is that the above anonymous wrapper function syntax currently only works for **macros** and *not* for functions. It should not be needed for functions, but sharing here for awareness.