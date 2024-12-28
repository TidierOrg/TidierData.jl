# ## Native (and preferred) method of interpolating using `@eval` and `$`

# TidierData relies on "non-standard evaluation," which has the side effect of making interpolation slightly more complicated. For example, in the expression `@mutate(df, a = b + 1)`, the `df` refers to a data frame, while `a` and `b` refer to column names within the data frame. What would happen if you created a variable `var` that contains the value `:a`. Would this interpolated expression work?

# ```julia
# using TidierData
# df = DataFrame(a = 1:5, b = 6:10)
# 
# var = :a
# @mutate(df, $var = b + 1)
# ```

# Unfortunately, this does not work because it produces `@mutate(df, :a = b + 1)`. Since TidierData uses bare variables (and not symbols) to refer to column names, this will result in an error. However, there is a slight modification we can apply to make this code work: prefixing it with an `@eval`.

using TidierData
df = DataFrame(a = 1:5, b = 6:10, c = 11:15)

var = :a
@eval @mutate(df, $var = b + 1)

# ### Why does adding an `@eval` to the beginning of the expression make interpolation work?

# Adding `@eval` to the beginning causes the interpolated expressions to be evaluated prior to be interpolated. So `$var`, which contains the value `:a`, is evaluated to `a`, which produces the desired expression `@mutate(df, a = b + 1)`. The need of `@eval` here then is primarily because TidierData expects an `a` rather than an `:a` to refer to the column "a" in a data frame.

# ### How can I use `@eval` with a chained set of expressions?

# The answer is simple: use `@eval @chain` instead of `@chain`.

var = :a

@eval @chain df begin
  @select($var)
  @mutate($var = $var + 1)
end

# If you want to select multiple variables, just use a `...` to splat the vector (or tuple) of variables.

vars = [:a, :b]

@eval @chain df begin
  @select($vars...)
end

# The `@eval`-based interpolation syntax is highly flexible in that it should work anywhere you might need it across the entire package.

@eval @chain df begin
  @summarize(across($vars..., mean))
end

# ### Does `@eval` work inside of user-defined functions?

# Yes. Here's an example of how you could roll up a new `select_new` function wrapping the `@select` macros.

function select_new(df, columns...)
  @eval @select(df, $columns...)
end

select_new(df, :a, :c)

# Yes. Here's another example of an `add_one()` function that adds one to all numeric columns and returns the result in a new set of columns.

function add_one(df)
  @eval @mutate(df, across(where(is_number), x -> x .+ 1))
end

add_one(df)

# ## Note: the below documentation is included here only for historical reasons. It will be removed in the future.

# ## Superseded method of interpolating using the `!!` ("bang bang") operator

# The `!!` ("bang bang") operator can be used to interpolate values of variables from the parent environment into your code. This operator is borrowed from the R `rlang` package. At some point, we may switch to using native Julia interpolation, but for a variety of reasons that introduce some complexity with native interpolation, we plan to continue to support `!!` interpolation.

# To interpolate multiple variables, the `rlang` R package uses the `!!!` "triple bang" operator. However, in `TidierData.jl`, the `!!` "bang bang" operator can be used to interpolate either single or multiple values as shown in the examples below.

# Note: You can only interpolate values from variables in the parent environment. If you would like to interpolate column names, you have two options: you can either use `across()` or you can use `@aside` with `@pull()` to create variables in the parent environment containing the values of those columns which can then be accessed using interpolatino.

# myvar = :b` and `myvar = Cols(:a, :b)` both refer to *columns* with those names. On the other hand, `myvar = "b"`, `myvar = ("a", "b")` and `myvar = ["a", "b"]` will interpolate the *values*. If you intend to interpolate column names, the preferred way is to use `Cols()` as in the examples below.

using TidierData

df = DataFrame(a = string.(repeat('a':'e', inner = 2)),
               b = [1,1,1,2,2,2,3,3,3,4],
               c = 11:20)

# ## Select the column (because `myvar` contains a symbol)

myvar = :b

@chain df begin
  @select(!!myvar)
end

# ## Select multiple variables

# You can also use a vector as in `[:a, :b]`, but `Cols()` is preferred because it lets you mix and match numbers.

myvars = Cols(:a, :b)

@chain df begin
  @select(!!myvars)
end

# This is the same as this...

myvars = Cols(:a, 2)

@chain df begin
  @select(!!myvars)
end

# ## Filter rows containing the *value* of `myvar_string`

myvar_string = "b"

@chain df begin
  @filter(a == !!myvar_string)
end

# ## Filtering rows works similarly using `in`.

# Note that for `in` to work here, we have to wrap it in `[]` because otherwise, the string will be converted into a collection of characters, which are a different data type.

myvar_string = "b"

@chain df begin
  @filter(a in [!!myvar_string])
end

# ## You can also use this for a vector (or tuple) of strings.

myvars_string = ["a", "b"]

@chain df begin
  @filter(a in !!myvars_string)
end

# ## Mutate one variable

# Remember: You cannot interpolate column names into `@mutate()` expressions. However, you *can* create a temporary variable containing the values of the column in question *or* you can use `@mutate()` with `across()`.

# ### Option 1: Create a temporary variable containing the values of the column.

myvar = :b

@chain df begin
  @aside(myvar_values = @pull(_, !!myvar))
  @mutate(d = !!myvar_values + 1)
end

# ### Option 2: Use `@mutate()` with `across()`

# Note: when using `across()`, anonymous functions are not vectorized. This is intentional to allow users to specify their function exactly as desired.

@chain df begin
  @mutate(across(!!myvar, x -> x .+ 1))
  @rename(d = b_function)
end

# ## Summarize across one variable

myvar = :b

@chain df begin
  @summarize(across(!!myvar, mean))
end

# ## Summarize across multiple variables

myvars = Cols(:b, :c)

@chain df begin
  @summarize(across(!!myvars, (mean, minimum, maximum)))
end

# ## Group by one interpolated variable

myvar = :a

@chain df begin
  @group_by(!!myvar)
  @summarize(c = mean(c))
end

# ## Group by multiple interpolated variables

# Once again, you can mix and match column selectors within `Cols()`

myvars = Cols(:a, 2)

@chain df begin
  @group_by(!!myvars)
  @summarize(c = mean(c))
end

# Notice that `df` remains grouped by `a` because the `@summarize()` peeled off one layer of grouping.

# ## Global constants

# You can also use `!!` interpolation to access global variables like `pi`.

df = DataFrame(radius = 1:5)

@chain df begin
  @mutate(area = !!pi * radius^2)
end

# As of v0.14.0, global constants defined within the Base or Core modules (like `missing`, `pi`, and `Real` can be directly referenced without any `!!`)

@chain df begin
  @mutate(area = pi * radius^2)
end

# ## Alternative interpolation syntax

# Since we know that `pi` is defined in the `Main` module, we can also access it using `Main.pi`.

@chain df begin
  @mutate(area = Main.pi * radius^2)
end

# The key lesson with interpolation is that any bare unquoted variable is assumed to refer to a column name in the DataFrame. If you are referring to any variable outside of the DataFrame, you need to either use `!!variable` or `[Module_name_here].variable` syntax to refer to this variable.

# Note: You can use `!!` interpolation anywhere, including inside of functions and loops.

df = DataFrame(a = string.(repeat('a':'e', inner = 2)),
               b = [1,1,1,2,2,2,3,3,3,4],
               c = 11:20)

for col in [:b, :c]
  @chain df begin
    @summarize(across(!!col, mean))
    println
  end
end
