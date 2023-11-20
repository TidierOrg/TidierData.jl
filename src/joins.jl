"""
$docstring_left_join
"""
macro left_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    leftjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

macro left_join(df1, df2)
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    leftjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

"""
$docstring_right_join
"""
macro right_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    rightjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

macro right_join(df1, df2)
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    rightjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

"""
$docstring_inner_join
"""
macro inner_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    innerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

macro inner_join(df1, df2)
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    innerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

"""
$docstring_full_join
"""
macro full_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    outerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

macro full_join(df1, df2)
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    outerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

"""
$docstring_anti_join
"""
macro anti_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    antijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

macro anti_join(df1, df2)
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    antijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

"""
$docstring_semi_join
"""
macro semi_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    semijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end

macro semi_join(df1, df2)
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    semijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr)
  end
  return df_expr
end