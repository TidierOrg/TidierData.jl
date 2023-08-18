"""
docstring_is_categorical
"""
function is_float(column::AbstractVector)
    T = eltype(column)
    if T isa Union
        T = filter(t -> t != Missing, Base.uniontypes(T))[1]
    end
    return T <: AbstractFloat
end

"""
docstring_is_categorical
"""
function is_integer(column::AbstractVector)
    T = eltype(column)
    if T isa Union
        T = filter(t -> t != Missing, Base.uniontypes(T))[1]
    end
    return T <: Integer
end

"""
docstring_is_categorical
"""
function is_string(column::AbstractVector)
    T = eltype(column)
    if T isa Union
        T = filter(t -> t != Missing, Base.uniontypes(T))[1]
    end
    return T <: AbstractString
end

"""
docstring_is_categorical
"""
function is_categorical(column::AbstractVector)
    T = eltype(column)
    if T isa Union
        T = filter(t -> t != Missing, Base.uniontypes(T))[1]
    end
    return T <: CategoricalValue
end