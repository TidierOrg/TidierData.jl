# TidierData.jl

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/TidierOrg/TidierData.jl/blob/main/LICENSE)
[![Docs: Latest](https://img.shields.io/badge/Docs-Latest-blue.svg)](https://tidierorg.github.io/TidierData.jl/latest)
[![Build Status](https://github.com/TidierOrg/TidierData.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/TidierOrg/TidierData.jl/actions/workflows/CI.yml?query=branch%3Amain)
<!-- [![Downloads](https://shields.io/endpoint?url=https://pkgs.genieframework.com/api/v1/badge/TidierData&label=Downloads)](https://pkgs.genieframework.com?packages=TidierData) -->

<img src="/docs/src/assets/Tidier_jl_logo.png" align="right" style="padding-left:10px;" width="150"/>

## What is TidierData.jl?

TidierData.jl is a 100% Julia implementation of the dplyr and tidyr R packages. Powered by the DataFrames.jl package and Julia’s
extensive meta-programming capabilities, TidierData.jl is an R user’s love
letter to data analysis in Julia.

`TidierData.jl` has three goals, which differentiate it from other data analysis
meta-packages in Julia:

1.  **Stick as closely to dplyr and tidyr syntax as possible:** Whereas other
    meta-packages introduce Julia-centric idioms for working with
    DataFrames, this package’s goal is to reimplement dplyr and tidyr
    in Julia. This means that `TidierData.jl` uses *tidy expressions* as opposed
    to idiomatic Julia expressions. An example of a tidy expression is
    `a = mean(b)`.

2.  **Make broadcasting mostly invisible:** Broadcasting trips up many R
    users switching to Julia because R users are used to most functions
    being vectorized. `TidierData.jl` currently uses a lookup table to decide
    which functions *not* to vectorize; all other functions are
    automatically vectorized. Read the documentation page on "Autovectorization"
    to read about how this works, and how to override the defaults.

3.  **Make scalars and tuples mostly interchangeable:** In Julia, the function
    `across(a, mean)` is dispatched differently than `across((a, b), mean)`.
    The first argument in the first instance above is treated as a scalar,
    whereas the second instance is treated as a tuple. This can be very confusing
    to R users because `1 == c(1)` is `TRUE` in R, whereas in Julia `1 == (1,)`
    evaluates to `false`. The design philosophy in `TidierData.jl` is that the user
    should feel free to provide a scalar or a tuple as they see fit anytime
    multiple values are considered valid for a given argument, such as in
    `across()`, and `TidierData.jl` will figure out how to dispatch it.

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

## What functions does TidierData.jl support?

To support R-style programming, TidierData.jl is implemented using macros.

TidierData.jl currently supports the following top-level macros:

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
- `@drop_missing()` and `@fill_missing()`
- `@unnest_longer()`, `@unnest_wider()`, and `@nest()`
- `@clean_names()` (as in R's `janitor::clean_names()` function)
- `@summary()` (as in R's `summary()` function)

TidierData.jl also supports the following helper functions:

- `across()`
- `where()`
- `desc()`
- `if_else()` and `case_when()`
- `n()` and `row_number()`
- `ntile()`
- `lag()` and `lead()`
- `everything()`, `starts_with()`, `ends_with()`, `matches()`, and `contains()`
- `as_float()`, `as_integer()`, and `as_string()`
- `is_number()`, `is_float()`, `is_integer()`, and `is_string()`
- `missing_if()` and `replace_missing()`

See the documentation [Home](https://tidierorg.github.io/TidierData.jl/latest/) page for a guide on how to get started, or the [Reference](https://tidierorg.github.io/TidierData.jl/latest/reference/) page for a detailed guide to each of the macros and functions.

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