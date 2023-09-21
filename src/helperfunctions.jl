# This file is intended for any catch-all helper functions that don't deserve
# their own documentation page and don't have any outside licenses.

# These are aliases for existing Julia functions
"""
$docstring_starts_with
"""
starts_with(args...) = startswith(args...)

"""
$docstring_ends_with
"""
ends_with(args...) = endswith(args...)

"""
$docstring_matches
"""
matches(pattern, flags...) = Regex(pattern, flags...)

"""
$docstring_everything
"""
everything() = All()