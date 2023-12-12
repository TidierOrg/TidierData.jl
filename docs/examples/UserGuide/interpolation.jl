# The `!!` ("bang bang") operator can be used to interpolate values of variables from the parent environment into your code. This operator is borrowed from the R `rlang` package. At some point, we may switch to using native Julia interpolation, but for a variety of reasons that introduce some complexity with native interpolation, we plan to continue to support `!!` interpolation.

# To interpolate multiple variables, the `rlang` R package uses the `!!!` "triple bang" operator. However, in `TidierData.jl`, the `!!` "bang bang" operator can be used to interpolate either single or multiple values as shown in the examples below.

# Note: You can only interpolate values from variables in the parent environment. If you would like to interpolate column names, you have two options: you can either use `across()` or you can use `@aside` with `@pull()` to create variables in the parent environment containing the values of those columns which can then be accessed using interpolatino.

# myvar = :b`, `myvar = (:a, :b)`, and `myvar = [:a, :b]` all refer to *columns* with those names. On the other hand, `myvar = "b"`, `myvar = ("a", "b")` and `myvar = ["a", "b"]` will interpolate those *values*. See below for examples.

using TidierData

df = DataFrame(a = string.(repeat('a':'e', inner = 2)),
               b = [1,1,1,2,2,2,3,3,3,4],
               c = 11:20)

# ## Select the column (because `myvar` contains a symbol)

myvar = :b

@chain df begin
  @select(!!myvar)
end

# ## Select multiple variables (vector of symbols)

myvars = [:a, :b]

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

myvars = [:b, :c]

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

myvars = [:a, :b]

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
