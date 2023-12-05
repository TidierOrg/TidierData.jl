# TidierData.jl uses a lookup table to decide which functions *not* to vectorize. For example, `mean()` is listed as a function that should never be vectorized. Also, any function used inside of `across()` is also not automatically vectorized. Any function that is not included in this list *and* is used in a context other than `across()` is automatically vectorized.

# Which functions are not vectorized? The set of non-vectorized functions is contained in the array `TidierData.not_vectorized[]`. Let's take a look at this array. We will wrap it in a `string()` to make the output easier to read.

using TidierData

string(TidierData.not_vectorized[])

# This "auto-vectorization" makes working with TidierData.jl more R-like and convenient. However, if you ever define your own function and try to use it, TidierData.jl may unintentionally vectorize it for you. To prevent auto-vectorization, you can prefix your function with a `~`.

df = DataFrame(a = repeat('a':'e', inner = 2), b = [1,1,1,2,2,2,3,3,3,4], c = 11:20)

# For example, let's define a function `new_mean()` that calculates a mean.

new_mean(exprs...) = mean(exprs...)

# If we try to use `new_mean()` inside of `@mutate()`, it will give us the wrong result. This is because `new_mean()` is vectorized, which results in the mean being calculated element-wise, which is almost never what we actually want.

@chain df begin
    @mutate(d = c - new_mean(c))
end

# To prevent `new_mean()` from being vectorized, we need to prefix it with a `~` like this:

@chain df begin
    @mutate(d = c - ~new_mean(c))
end

# Or you can modify the do-not-vectorize list like this:

push!(TidierData.not_vectorized[], :new_mean)

# Now `new_mean()` should behave just like `mean()` in that it is treated as non-vectorized.

@chain df begin
    @mutate(d = c - new_mean(c))
end

# This gives us the correct answer. Notice that adding a `~` is not needed with `mean()` because `mean()` is already included on our look-up table of functions not requiring vectorization.

@chain df begin
    @mutate(d = c - mean(c))
end

# If you're not sure if a function is vectorized and want to prevent it from being vectorized, you can always prefix it with a ~ to prevent vectorization. Even though `mean()` is not vectorized anyway, prefixing it with a ~ will not cause any harm.

@chain df begin
    @mutate(d = c - ~mean(c))
end

# If for some crazy reason, you *did* want to vectorize `mean()`, you are always allowed to vectorize it, and TidierData.jl won't un-vectorize it.

@chain df begin
    @mutate(d = c - mean.(c))
end

# Note: `~` also works with operators, so if you want to *not* vectorize an operator, you can prefix it with `~`, for example, `a ~* b` will perform a matrix multiplication rather than element-wise multiplication.