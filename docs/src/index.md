
<img src="assets/Tidier\_jl\_logo.png" align="left" style="padding-right:10px"; width="150"></img>

## What is TidierData.jl?

TidierData.jl is a 100% Julia implementation of the dplyr and tidyr R packages.
Powered by the DataFrames.jl package and Julia’s
extensive meta-programming capabilities, TidierData.jl is an R user’s love
letter to data analysis in Julia.

`TidierData.jl` has three goals, which differentiate it from other data analysis
meta-packages in Julia:

```@raw html
??? tip "Stick as closely to tidyverse syntax as possible."
    Whereas other meta-packages introduce Julia-centric idioms for working with
    DataFrames, this package’s goal is to reimplement parts of tidyverse
    in Julia. This means that `TidierData.jl` uses *tidy expressions* as opposed
    to idiomatic Julia expressions. An example of a tidy expression is
    `a = mean(b)`. In Julia, `a` and `b` are variables and are thus "eagerly"
    evaluated. This means that if `b` is merely referring to a column in a
    data frame and *not* an object in the global namespace, then an error
    will be generated because `b` was not found. In idiomatic Julia, `b`
    would need to be expressed as a symbol, or `:b`. Even then,
    `a = mean(:b)` would generate an error because it's not possible to
    calculate the mean value of a symbol. To handle this using idiomatic
    Julia, `DataFrames.jl` introduces a mini-language that relies heavily
    on the creation of anonymous functions, with explicit directional
    pairs syntax using a `source => function => destination` syntax. While
    this is quite elegant, it can be verbose. `TidierData.jl` aims to
    reduce this complexity by exposing an R-like syntax, which is then
    converted into valid `DataFrames.jl` code. The reason that
    *tidy expressions* are considered valid by Julia in `TidierData.jl` is
    because they are implemented using macros. Macros "capture" the
    expressions they are given, and then they can modify those expressions
    before evaluating them. For consistency, all top-level `dplyr` functions
    are implemented as macros (whether or not a macro is truly needed), and
    all "helper" functions (used inside of those top-level functions) are
    implemented as functions or pseudo-functions (functions which only exist
    through modification of the abstract syntax tree).
```

```@raw html
??? tip "Make broadcasting mostly invisible."
    Broadcasting trips up many R users switching to Julia because R users are used to most functions being vectorized. `TidierData.jl` currently uses a lookup table to decide which functions *not* to vectorize; all other functions are automatically vectorized. Read the documentation page on "Autovectorization" to read about how this works, and how to override the defaults. An example of where this issue commonly causes errors is when centering a variable. To create a new column `a` that centers the column `b`, `TidierData.jl` lets you simply write `a = b - mean(b)` exactly as you would in R. This works because `TidierData.jl` knows to *not* vectorize `mean()` while also recognizing that `-` *should* be vectorized such that this expression is rewritten in `DataFrames.jl` as `:b => (b -> b .- mean(b)) => :a`. For any user-defined function that you want to "mark" as being non-vectorized, you can prefix it with a `~`. For example, a function `new_mean()`, if it had the same functionality as `mean()` *would* normally get vectorized by `TidierData.jl` unless you write it as `~new_mean()`.
```

```@raw html
??? tip "Make scalars and tuples mostly interchangeable."
    In Julia, the function `across(a, mean)` is dispatched differently than `across((a, b), mean)`. The first argument in the first instance above is treated as a scalar, whereas the second instance is treated as a tuple. This can be very confusing to R users because `1 == c(1)` is `TRUE` in R, whereas in Julia `1 == (1,)` evaluates to `false`. The design philosophy in `TidierData.jl` is that the user should feel free to provide a scalar or a tuple as they see fit anytime multiple values are considered valid for a given argument, such as in `across()`, and `TidierData.jl` will figure out how to dispatch it.
```

## Installation

For the stable version:

```
] add TidierData
```

The `]` character starts the Julia [package manager](https://docs.julialang.org/en/v1/stdlib/Pkg/). Press the backspace key to return to the Julia prompt.

or


```julia
using Pkg
Pkg.add("TidierData")
```

For the newest version:

```
] add TidierData#main
```

or

```julia
using Pkg
Pkg.add(url="https://github.com/TidierOrg/TidierData.jl")
```

## What macros and functions does TidierData.jl support?

To support R-style programming, `TidierData.jl` is implemented using macros. This is because macros are able to "capture" the code before executing it, which allows the package to support R-like "tidy expressions" that would otherwise not be considered valid Julia code.

TidierData.jl currently supports the following top-level macros:

```@raw html
!!! example "Top-level macros:"
    - `@glimpse()`
    - `@select()` and `@distinct()`
    - `@rename()` and `@rename_with()`
    - `@mutate()` and `@transmute()` 
    - `@summarize()` and `@summarise()`
    - `@filter()`
    - `@slice()`, `@slice_sample()`, `@slice_min()`, `@slice_max()`, `@slice_head()`, and `@slice_tail()`
    - `@group_by()` and `@ungroup()`
    - `@arrange()`
    - `@pull()`
    - `@count()` and `@tally()`
    - `@left_join()`, `@right_join()`, `@inner_join()`, `@full_join()`, `@anti_join()`, and `@semi_join()`
    - `@bind_rows()` and `@bind_cols()`
    - `@pivot_wider()` and `@pivot_longer()`
    - `@separate()`, `@separate_rows()`, and `@unite()`
    - `@drop_missing()` and `@fill_missing`
    - `@clean_names()` (as in R's `janitor::clean_names()` function)
    - `@summary()` (as in R's `summary()` function)
```
TidierData.jl also supports the following helper functions:

```@raw html
!!! example "Helper functions:"
    - `across()`
    - `desc()`
    - `if_else()` and `case_when()`
    - `n()` and `row_number()`
    - `ntile()`
    - `lag()` and `lead()`
    - `everything()`, `starts_with()`, `ends_with()`, `matches()`, and `contains()`
    - `as_float()`, `as_integer()`, and `as_string()`
    - `is_float()`, `is_integer()`, and `is_string()`
    - `missing_if()` and `replace_missing()`
```

See the [Reference](https://tidierorg.github.io/TidierData.jl/latest/reference/) page for a detailed guide to each of the macros and functions.

## Example

Let's select the first five movies in our dataset whose budget exceeds the mean budget. Unlike in R, where we pass an `na.rm = TRUE` argument to remove missing values, in Julia we wrap the variable with a `skipmissing()` to remove the missing values before the `mean()` is calculated.

```julia
using TidierData
using RDatasets

movies = dataset("ggplot2", "movies");

@chain movies begin
    @mutate(Budget = Budget / 1_000_000)
    @filter(Budget >= mean(skipmissing(Budget)))
    @select(Title, Budget)
    @slice(1:5)
end
```

```
5×2 DataFrame
 Row │ Title                       Budget   
     │ String                      Float64? 
─────┼──────────────────────────────────────
   1 │ 'Til There Was You              23.0
   2 │ 10 Things I Hate About You      16.0
   3 │ 102 Dalmatians                  85.0
   4 │ 13 Going On 30                  37.0
   5 │ 13th Warrior, The               85.0
```

## What’s new

See [NEWS.md](https://github.com/TidierOrg/TidierData.jl/blob/main/NEWS.md) for the latest updates.

## What's missing

Is there a tidyverse feature missing that you would like to see in TidierData.jl? Please file a GitHub issue. Because TidierData.jl primarily wraps DataFrames.jl, our decision to integrate a new feature will be guided by how well-supported it is within DataFrames.jl and how likely other users are to benefit from it.