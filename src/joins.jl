"""
$docstring_left_join
"""
macro left_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    if log[] 
      log_join_changes( DataFrame($(esc(df1))), leftjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); 
      join_type="@left_join") 
    end
    leftjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end

macro left_join(df1, df2)
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    if log[] 
      log_join_changes( DataFrame($(esc(df1))), leftjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); 
      join_type="@left_join") 
    end
        leftjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end

"""
$docstring_right_join
"""
macro right_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    log[] ? log_join_changes( DataFrame($(esc(df1))), rightjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); join_type="@right_join") : nothing
    rightjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end

macro right_join(df1, df2)
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    log[] ? log_join_changes( DataFrame($(esc(df1))), rightjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); join_type="@right_join") : nothing
    rightjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end

"""
$docstring_inner_join
"""
macro inner_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    log[] ? log_join_changes( DataFrame($(esc(df1))), innerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); join_type="@inner_join") : nothing
    innerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end

macro inner_join(df1, df2)
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    log[] ? log_join_changes( DataFrame($(esc(df1))), innerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); join_type="@inner_join") : nothing
    innerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end

"""
$docstring_full_join
"""
macro full_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    log[] ? log_join_changes( DataFrame($(esc(df1))), outerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); join_type="@full_join") : nothing
    outerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end

macro full_join(df1, df2)
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    log[] ? log_join_changes( DataFrame($(esc(df1))), outerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); join_type="@full_join") : nothing
    outerjoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end

"""
$docstring_anti_join
"""
macro anti_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    log[] ? log_join_changes( DataFrame($(esc(df1))), antijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); join_type="@anti_join") : nothing 
    antijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end

macro anti_join(df1, df2)
  
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    log[] ? log_join_changes( DataFrame($(esc(df1))), antijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); join_type="@anti_join") : nothing
    antijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end

"""
$docstring_semi_join
"""
macro semi_join(df1, df2, by)
  by = parse_join_by(by)

  df_expr = quote
    log[] ? log_join_changes( DataFrame($(esc(df1))), semijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); join_type="@semi_join") : nothing

    semijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end

macro semi_join(df1, df2)
  by = :(intersect(names(DataFrame($(esc(df1)))), names(DataFrame($(esc(df2))))))

  df_expr = quote
    log[] ? log_join_changes( DataFrame($(esc(df1))), semijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by); join_type="@semi_join") : nothing

    semijoin(DataFrame($(esc(df1))), DataFrame($(esc(df2))); on = $by)
  end
  if code[]
    @info MacroTools.prettify(df_expr) # COV_EXCL_LINE
  end
  return df_expr
end