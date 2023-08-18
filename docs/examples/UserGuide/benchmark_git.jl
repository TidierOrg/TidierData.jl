# The goal of this benchmarking is to guage how Tidier.jl performs in comparison to DataFrames.jl. Ultimatley, from this benchmarking, we see that Tidier.jl is comprable in speed to DataFrames.jl

# ## Why function wrap?
# Wrapping code in a function allows it to compile just once, which more closely reflects the reality of production workflows. For a more robust explanation, please see @kdpsingh comment here: https://github.com/TidierOrg/TidierData.jl/issues/24#issuecomment-1682718061

using Tidier
using RDatasets
using BenchmarkTools

movies = dataset("ggplot2", "movies");

# ## filtering
function filtering_tidier()
@chain movies begin
    @filter(Year > 1939 && Votes > 40)
end
end

filtering_results = @benchmark filtering_tidier()
```
BenchmarkTools.Trial: 1481 samples with 1 evaluation.
 Range (min … max):  1.850 ms … 13.948 ms  ┊ GC (min … max):  0.00% … 77.48%
 Time  (median):     2.979 ms              ┊ GC (median):     0.00%
 Time  (mean ± σ):   3.369 ms ±  1.694 ms  ┊ GC (mean ± σ):  11.25% ± 16.00%

       ▅█▇▅▄▂                                                 
  ▇▇▇▇▇████████▅▄▆▄▄▁▁▁▁▁▁▁▁▁▁▁▁▁▄▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▄▄▆▆█▇▇▇ █
  1.85 ms      Histogram: log(frequency) by time     11.1 ms <

 Memory estimate: 3.40 MiB, allocs estimate: 283.
```
@benchmark filter(row -> row.Year > 1939 && row.Votes > 40, movies)
```
BenchmarkTools.Trial: 453 samples with 1 evaluation.
 Range (min … max):   9.731 ms … 15.395 ms  ┊ GC (min … max): 0.00% … 25.19%
 Time  (median):     10.702 ms              ┊ GC (median):    0.00%
 Time  (mean ± σ):   11.033 ms ±  1.133 ms  ┊ GC (mean ± σ):  3.69% ±  8.86%

    ▃▃ ▄█▁                                                     
  ▃▆██▇████▆▆▅▆▄▆▄▆▅▅▅▇▅▆▇█▆▄▆▁▂▂▁▂▁▁▁▁▁▂▁▁▃▃▃▂▁▄▂▂▄▃▂▃▄▃▃▃▂▂ ▃
  9.73 ms         Histogram: frequency by time        14.3 ms <

 Memory estimate: 7.76 MiB, allocs estimate: 287666.
```
# ## group_by summarize
function groupbysummarize_tidier()
@chain movies begin
    @group_by(MPAA)
    @summarise(n=n())
end
end

groupbysummarize_results = @benchmark groupbysummarize_tidier()
```
BenchmarkTools.Trial: 2888 samples with 1 evaluation.
 Range (min … max):  590.502 μs … 27.591 ms  ┊ GC (min … max):  0.00% … 95.79%
 Time  (median):       1.508 ms              ┊ GC (median):     0.00%
 Time  (mean ± σ):     1.728 ms ±  2.130 ms  ┊ GC (mean ± σ):  11.14% ±  8.53%

                        ▆█▄▂▁▁                                  
  ▃▃▂▂▂▂▂▂▂▁▁▂▁▁▁▁▂▂▂▂▂▇███████▇▆▄▄▃▃▃▃▃▂▂▂▂▂▂▂▂▂▂▁▂▁▁▂▂▁▁▂▂▂▂ ▃
  591 μs          Histogram: frequency by time         2.77 ms <

 Memory estimate: 1.91 MiB, allocs estimate: 264.
```
@benchmark combine(groupby(movies, :MPAA), nrow => :n)
```
BenchmarkTools.Trial: 9512 samples with 1 evaluation.
 Range (min … max):  244.088 μs …   9.063 ms  ┊ GC (min … max): 0.00% … 92.39%
 Time  (median):     452.044 μs               ┊ GC (median):    0.00%
 Time  (mean ± σ):   520.768 μs ± 583.926 μs  ┊ GC (mean ± σ):  8.35% ±  7.10%

  ▄▄▂▂▂▁      ▅██▇▆▆▆▅▄▃▂▃▄▃▂▁▂▁   ▁▂▂▂▁                        ▂
  ████████▆▆▆▆███████████████████▇▇██████▇▇▇▆▆▆▅▅▄▃▃▅▃▂▃▃▅▆▅▂▄▅ █
  244 μs        Histogram: log(frequency) by time       1.08 ms <

 Memory estimate: 474.59 KiB, allocs estimate: 267.
```

# ## one mutate
function mutate_1_tidier()
@chain movies begin
    @mutate(new_col = Votes * R1)
end
end

@benchmark mutate_1_tidier()
```
BenchmarkTools.Trial: 787 samples with 1 evaluation.
 Range (min … max):  2.613 ms … 30.255 ms  ┊ GC (min … max):  0.00% … 80.60%
 Time  (median):     5.516 ms              ┊ GC (median):     0.00%
 Time  (mean ± σ):   6.346 ms ±  4.219 ms  ┊ GC (mean ± σ):  13.68% ± 16.07%

       ▅█▅                                                    
  ▅▄▄▅▅████▆▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▄▄▄▅▆▆▅ ▇
  2.61 ms      Histogram: log(frequency) by time       29 ms <

 Memory estimate: 8.42 MiB, allocs estimate: 266.
```
@benchmark transform(movies, [:Votes, :R1] => ((v, r) -> v .* r) => :new_col)
```
BenchmarkTools.Trial: 855 samples with 1 evaluation.
 Range (min … max):  2.421 ms … 12.714 ms  ┊ GC (min … max):  0.00% … 53.52%
 Time  (median):     5.343 ms              ┊ GC (median):     0.00%
 Time  (mean ± σ):   5.841 ms ±  1.813 ms  ┊ GC (mean ± σ):  13.42% ± 21.07%

                  ▆█▇▄▁                                       
  ▆▅▆▅▄▆▅▆▅▆▄▆▆▅▆██████▆▅▄▁▁▁▁▁▁▁▄▁▁▁▁▁▁▁▁▅▆▄▇▇▆▆▇▅▅▆▆▅█▆▅▇▇ ▇
  2.42 ms      Histogram: log(frequency) by time     11.9 ms <

 Memory estimate: 8.42 MiB, allocs estimate: 267.
```

# ## mutate 6 new columns
function mutate6_tidier()
    @chain movies begin
        @mutate(
        Votes_R1_Product = Votes .* R1, 
        Rating_Year_Ratio = Rating ./ Year, 
        R1_to_R5_Sum = R1 + R2 + R3 + R4 + R5, 
        High_Budget_Flag = if_else(ismissing(Budget), "NA", Budget .> 50000),
        R6_to_R8_Avg = (R6 + R7 + R8) / 3, 
        year_Minus_Length = Year - Length)
    end
end

@benchmark mutate6_tidier()
```
BenchmarkTools.Trial: 592 samples with 1 evaluation.
 Range (min … max):  5.037 ms … 31.335 ms  ┊ GC (min … max):  0.00% … 78.87%
 Time  (median):     7.405 ms              ┊ GC (median):     0.00%
 Time  (mean ± σ):   8.443 ms ±  4.679 ms  ┊ GC (mean ± σ):  13.34% ± 17.27%

      ▇█▆▁                                                    
  ▅▆▆▄████▆▁▅▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▆▇▇▆▆ ▇
  5.04 ms      Histogram: log(frequency) by time     30.1 ms <

 Memory estimate: 10.92 MiB, allocs estimate: 852.
 ```
@benchmark transform(movies, [:Votes, :R1] => ((v, r) -> v .* r) => :Votes_R1_Product, [:Rating, :Year] => ((r, y) -> r ./ y) => :Rating_Year_Ratio, [:R1, :R2, :R3, :R4, :R5] => ((a, b, c, d, e) -> a + b + c + d + e) => :R1_to_R5_Sum, :Budget => (b -> ifelse.(ismissing.(b), missing, b .> 50000)) => :High_Budget_Flag, [:R6, :R7, :R8] => ((f, g, h) -> (f + g + h) / 3) => :R6_to_R8_Avg, [:Year, :Length] => ((y, l) -> y - l) => :Year_Minus_Length )
```
BenchmarkTools.Trial: 620 samples with 1 evaluation.
 Range (min … max):  4.314 ms … 34.952 ms  ┊ GC (min … max):  0.00% … 84.05%
 Time  (median):     7.009 ms              ┊ GC (median):     0.00%
 Time  (mean ± σ):   8.049 ms ±  4.765 ms  ┊ GC (mean ± σ):  13.61% ± 17.03%

     ▃▆█▇▅▃▂                                                  
  ▆▅▆███████▅▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▄▆▁▄▁▄▄▆▇▅▆▅ ▇
  4.31 ms      Histogram: log(frequency) by time     30.8 ms <

 Memory estimate: 10.58 MiB, allocs estimate: 881.
```

# ## groupby then 2 mutates

function groupby1_2mutate_tidier()
@chain movies begin 
    @group_by(MPAA)
    @mutate(ace = R1 -> R1/2 * 4)
    @mutate(Bace = Votes^R1)
end 
end

@benchmark groupby1_2mutate_tidier()
```
BenchmarkTools.Trial: 255 samples with 1 evaluation.
 Range (min … max):  16.388 ms … 42.411 ms  ┊ GC (min … max):  0.00% … 56.51%
 Time  (median):     17.413 ms              ┊ GC (median):     0.00%
 Time  (mean ± σ):   19.728 ms ±  6.776 ms  ┊ GC (mean ± σ):  11.67% ± 17.25%

  ▄█▇▆▄▁                                                       
  ██████▆▁▁▁▁▁▄▁▄▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▆▄▄▆▆▆█▄█ ▆
  16.4 ms      Histogram: log(frequency) by time      40.9 ms <

 Memory estimate: 23.52 MiB, allocs estimate: 2524.
```
@benchmark transform( transform( groupby(movies, :MPAA), :R1 => (x -> x/2 * 4) => :ace, ungroup = false), [:Votes, :R1] => ((a, b) -> b .^ a) => :Bace, ungroup = false)
```
BenchmarkTools.Trial: 231 samples with 1 evaluation.
 Range (min … max):  17.881 ms … 42.777 ms  ┊ GC (min … max):  0.00% … 55.93%
 Time  (median):     19.118 ms              ┊ GC (median):     0.00%
 Time  (mean ± σ):   21.641 ms ±  6.783 ms  ┊ GC (mean ± σ):  11.28% ± 17.05%

   ▄█▄                                                         
  ▆███▇▅▄▄▃▃▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▂▃▃▃▃▃▃▃▃ ▃
  17.9 ms         Histogram: frequency by time        41.7 ms <

 Memory estimate: 26.17 MiB, allocs estimate: 2543.
```
# ## select 5 columns
function select5_tidier()
    @chain movies begin 
        @select(R1:R5)
    end 
end

@benchmark select5_tidier()
```
BenchmarkTools.Trial: 3380 samples with 1 evaluation.
 Range (min … max):  160.592 μs … 27.372 ms  ┊ GC (min … max):  0.00% … 96.21%
 Time  (median):       1.262 ms              ┊ GC (median):     0.00%
 Time  (mean ± σ):     1.480 ms ±  2.221 ms  ┊ GC (mean ± σ):  15.01% ±  9.44%

                                 █▆▂ ▁▃                         
  ▃▂▂▂▂▁▂▂▂▂▁▁▁▁▁▂▂▂▁▁▁▁▂▂▁▁▁▁▁▂▆██████▇▄▄▃▃▂▂▂▂▂▂▂▂▂▂▁▂▂▂▂▁▁▂ ▃
  161 μs          Histogram: frequency by time         2.12 ms <

 Memory estimate: 2.24 MiB, allocs estimate: 35.
 ```

 @benchmark select(movies, :R1, :R2, :R3, :R4, :R5)
```
BenchmarkTools.Trial: 2980 samples with 1 evaluation.
 Range (min … max):  236.114 μs … 29.011 ms  ┊ GC (min … max):  0.00% … 96.73%
 Time  (median):       1.430 ms              ┊ GC (median):     0.00%
 Time  (mean ± σ):     1.671 ms ±  2.408 ms  ┊ GC (mean ± σ):  14.35% ±  9.39%

  ▂          ▇▇█▆▅▄▃▂                                          ▁
  ██▇▅▃▆▃▁▅▃▄█████████▇▅▃▄▁▁▁▁▃▁▁▁▁▁▁▁▁▃▁▁▃▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▃ █
  236 μs        Histogram: log(frequency) by time      5.64 ms <

 Memory estimate: 2.26 MiB, allocs estimate: 280.
```
