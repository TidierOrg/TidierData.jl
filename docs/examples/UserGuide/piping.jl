# The easiest way to use TidierData.jl for complex data transformation operations is to connect them together using pipes. Julia comes with the built-in `|>` pipe operator, but TidierData.jl also includes and re-exports the `@chain` macro from the Chain.jl package. On this page, we will show you how to use both approaches.

# First, let's load a dataset.

using TidierData
using RDatasets

movies = dataset("ggplot2", "movies");

# ## Julia's built-in `|>` pipe

# If we wanted to figure out the number of rows in the `movies` data frame, one way to do this is to apply the `nrow()` function to movies. The most straightforward way is to write it like this:

nrow(movies)

# Another perfectly valid way to write this expression is by piping `movies` into `nrow` using the `|>` pipe operator.

movies |> nrow

# Why might we want to do this? Well, whereas the first expression would naturally be read as "Calculate the number of rows of movies," the second expression reads as "Start with movies, then calculate the number of rows." For a simple expression, these are easy enough to reason about. However, as we start to pipe more and more functions in a single expression, the piped version becomes much easier to reason about.

# One quick note about Julia's built-in pipe: writing `movies |> nrow()` would *not* be considered valid. This is because Julia's built-in pipe always expects a function and *not* a function call. Writing `nrow` by itself is *naming* the function, whereas writing `nrow()` is *calling* the function. This quickly becomes an issue once we want to supply arguments to the function we are calling.

# Consider another approach to calculating the number of rows:

size(movies, 1)

# In this case, the `size()` function returns a tuple of `(rows, columns)`, and if you supply an optional second argument specifying the index of the tuple, it returns only that dimension. In this case, we called `size()` with a second argument of `1`, indicating that we only wanted the function to return the number of rows.

# How would we write this using Julia's built-in pipe?

movies |> 
  x -> size(x, 1)

# You might have wanted to write `movies |> size(1)`, but because `size(1)` would represent a function *call*, we have to wrap the function call within an anonymous function, which is easily accomplished using the `x -> func(x, arg1, arg2)` syntax, where `func()` refers to any function and `arg1` and `arg2` refer to any additional arguments that are needed.

# Another way we could have accomplished this is to calculate `size`, which returns a tuple of `(rows, columns)`, and then to use an anonymous function to grab the first value. Since we are calculating `size` without any arguments, we can simply write `size` within the pipe. However, to grab the first value using the `x[1]` syntax, we have to define an anonymous function. Putting it all together, we get this approach to piping:

movies |>
  size |>
  x -> x[1]

# ## Using the `@chain` macro

# The `@chain` macro comes from the Chain.jl package and is included and re-exported by TidierData.jl. Let's do this same series of exercises using `@chain`.

# Let's calculate the number of rows using `@chain`.

@chain movies nrow

# One of the reasons we prefer the use of `@chain` in TidierData.jl is that it is so concise. There is no need for any operator. Another interesting thing is that `@chain` doesn't care whether you use a function *name* or a function *call*. Both approaches work. As a result, writing `nrow()` instead of `nrow` is equally valid using `@chain`.

@chain movies nrow()

# There are two options for writing out multi-row chains. The preferred approach is as follows, where the starting item is listed, followed by a `begin-end` block.

@chain movies begin
  nrow
  # additional operations here
end

# `@chain` also comes with a built-in placeholder, which is `\_`. To calculate the `size` and extract the first value, we can use this approach:

@chain movies begin
  size
  _[1]
end

# You don't have to list the data frame before the `begin-end` block. This is equally valid:

@chain begin
  movies
  size
  _[1]
end

# The only time this approach is preferred is when instead of simply naming the data frame, you are using a function to read in the data frame from a file or database. Because this function call may include the path of the file, which could be quite long, it's easier to write this on it's own line within the `begin-end` block.

# While the documentation for TidierData.jl follows the convention of placing piped functions on separate lines of code using `begin-end` blocks, this is purely convention for ease of readability. You could rewrite the code above without the `begin-end` block as follows:

@chain movies size _[1]

# For simple transformations, this approach is both concise and readable.

# ## Using `@chain` with TidierData.jl

# Returning to our convention of multi-line pipes, let's grab the first five movies that were released since 2000 and had a rating of at least 9 out of 10. Here is one way that we could write this:

@chain movies begin
    @filter(Year >= 2000 && Rating >= 9)
    @slice(1:5)
end

# Note: we generally prefer using `&&` in Julia because it is a "short-cut" operator. If the first condition evaluates to `false`, then the second condition is not even evaluated, which makes it faster (because it takes a short-cut).

# In the case of `@filter`, multiple conditions can be written out as separate expressions.

@chain movies begin
  @filter(Year >= 2000, Rating >= 9)
  @slice(1:5)
end

# Another to write this expression is take advantage of the fact that Julia macros can be called without parentheses. In this case, we will add back the `&&` for the sake of readability.

@chain movies begin
  @filter Year >= 2000 && Rating >= 9
  @slice 1:5
end

# Lastly, TidierData.jl also supports multi-line expressions within each of the macros that accept multiple expressions. So you could also write this as follows:

@chain movies begin
  @filter begin
    Year >= 2000
    Rating >= 9
  end
  @slice 1:5
end

# What's nice about this approach is that if you want to remove some criteria, you can easily comment out the relevant parts. For example, if you're willing to consider older movies, just comment out the `Year >= 2000`.

@chain movies begin
  @filter begin
    # Year >= 2000
    Rating >= 9
  end
  @slice 1:5
end

# ## Which approach to use?

# The purpose of this page was to show you that both Julia's native pipes and the `@chain` macro are perfectly valid and capable. We prefer the use of `@chain` because it is a bit more flexible and concise, with a syntax that makes it easy to comment out individual operations. We have adopted a similar `begin-end` block functionality within TidierData.jl itself, so that you can spread arguments out over multiple lines if you prefer. In the end, the choice is up to you!