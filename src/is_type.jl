"""
$docstring_is_float
"""
function is_float(column::AbstractVector)
    T = eltype(column)
    if T isa Union
        T = filter(t -> t != Missing, Base.uniontypes(T))[1]
    end
    return T <: AbstractFloat
end

"""
$docstring_is_integer
"""
function is_integer(column::AbstractVector)
    T = eltype(column)
    if T isa Union
        T = filter(t -> t != Missing, Base.uniontypes(T))[1]
    end
    return T <: Integer
end

"""
$docstring_is_string
"""
function is_string(column::AbstractVector)
    T = eltype(column)
    if T isa Union
        T = filter(t -> t != Missing, Base.uniontypes(T))[1]
    end
    return T <: AbstractString
end