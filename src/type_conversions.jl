"""
$docstring_as_float
"""
function as_float(value)

        passmissing(convert)(Float64, value)

end

function as_float(value::AbstractString)

        passmissing(parse)(Float64, value)

end

"""
$docstring_as_integer
"""
function as_integer(value)
        passmissing(floor)(value) |>      
        x -> passmissing(convert)(Int64, x)
end

function as_integer(value::AbstractString)
        passmissing(parse)(Float64, value) |>
        x -> passmissing(floor)(x) |>      
        x -> passmissing(convert)(Int64, x)
end

"""
$docstring_as_string
"""
function as_string(value)
  passmissing(string)(value)
end