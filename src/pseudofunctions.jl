# COV_EXCL_START
"""
$docstring_across
"""
function across(args...)
  throw("This function should only be called inside of TidierData.jl macros.")
end

"""
$docstring_where
"""
function where(args...)
  throw("This function should only be called inside of TidierData.jl macros.")
end

"""
$docstring_desc
"""
function desc(args...)
  throw("This function should only be called inside of @arrange().")
end

"""
$docstring_n
"""
function n()
  throw("This function should only be called inside of TidierData.jl macros.")
end

"""
$docstring_row_number
"""
function row_number()
  throw("This function should only be called inside of TidierData.jl macros.")
end

# COV_EXCL_STOP