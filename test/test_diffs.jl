@testset "diffs" begin
    remove_col = @select(test_df, name, num)
    add_col = @mutate(test_df, num2 = [2, 3, 4, 5])
    remove_rows = @filter(test_df, name == "A")
    transmute_col = @transmute(test_df, label = string.(label))
    transmute_col2 = @transmute(test_df, labelstring = string.(label))
    rename_col = @rename(test_df, l = label)
    summarize_col = @summarize(test_df, n = sum(num))
    group_col = @group_by(test_df, label)
    ungroup = @ungroup(group_col)
    double = @bind_rows(test_df, test_df)
    half = @distinct(double)
    df = DataFrame( dt1 = [missing, 0.2, missing, missing, 1, missing, 5, 6], dt2 = [0.3, 2, missing, 3, missing, 5, 6,missing])
    filled = @fill_missing(df, dt2, "down");
    grouped = @chain test_df @group_by(name) @mutate(mean = mean(num));
    
    @test "@select removed: [\"label\"] " ==
    TidierData.generate_log(test_df, remove_col, "@select", [:colchange])
    @test "@mutate: new variable \"num2\" with 4 unique values and 0.0% missing. \n\t" ==
    TidierData.generate_log(test_df, add_col, "@mutate", [:colchange])
    @test "@filter: removed 2 rows (50.0%), 2 rows remaining. " ==
    TidierData.generate_log(test_df, remove_rows, "@filter", [:rowchange])
    @test "@transmute removed: [\"name\", \"num\"] " ==
    TidierData.generate_log(test_df, transmute_col, "@transmute", [:colchange])
    @test "@transmute removed: [\"label\", \"name\", \"num\"] @transmute: new variable \"labelstring\" with 2 unique values and 0.0% missing. \n\t" == TidierData.generate_log(test_df, transmute_col2, "@transmute", [:colchange])
    @test "@transmute removed: [\"label\"] @transmute: new variable \"l\" with 2 unique values and 0.0% missing. \n\t" ==
    TidierData.generate_log(test_df, rename_col, "@transmute", [:colchange])
    @test "@summarize returned a DataFrame (1 row, 1 column). " ==
    TidierData.generate_log(test_df, summarize_col, "@summarize", [:newsize])
    @test "@group_by added groups: [\"label\"]" ==
    TidierData.generate_log(test_df, group_col, "@group_by", [:groups])
    @test "@ungroup removed groups: [\"label\"]" ==
    TidierData.generate_log(group_col, ungroup, "@ungroup", [:groups])
    @test "@distinct: removed 4 rows (50.0%), 4 rows remaining. " ==
    TidierData.generate_log(double, half, "@distinct", [:rowchange])
    df1 = DataFrame(a = ["a", "b"], b = 1:2); df2 = DataFrame(a = ["a", "c"], c = 3:4);
    @test """@left_join: added 1 new column(s): [\"c\"].\n\t- Dimension Change: 2×2 -> 2×3\n""" == 
    TidierData.log_join_changes(df1, leftjoin(df1, df2, on = :a), join_type = "@left_join")
    @test !isempty(@chain test_df @mutate( num2 = [2, 3, missing, 5], num5 = [5, 6, missing, 8])  @mutate( num2 = replace_missing(num2, 8)))
    @test TidierData.log_changed_columns(test_df, grouped; base_msg =  "") ==
    """@mutate: new variable \"mean\" with 2 unique values and 0.0% missing."""
    @test TidierData.log_changed_columns(df, filled; base_msg =  "", name = "@fill_missing") == 
    """@fill_missing: changed 3 values (38.0%) of \"dt2\" (3 replaced missing)"""
    dfu = DataFrame( b = ["1", "2", "3"], c = ["1", "2", "3"], d = [missing, missing, "3"]); dfud = @unite(dfu, new_col, (b, c, d), "-");
    @test TidierData.log_unite_changes(dfu, dfud, :new_col) == 
    "@unite: added variable \"new_col\" with 3 unique value(s), 0.0% missing\n\t\tremoved 3 columns: [\"b\", \"c\", \"d\"]\n"
    df = DataFrame(a = ["1-1", "2-2", "3-3-3"]); df2 = @separate(df, a, [b, c, d], "-")
    @test strip(TidierData.log_separate_changes(df, df2, [:b, :c, :d])) ==
      strip("""
@separate added 3 new columns and removed column \"a\" \n\t- b: not found in the output DataFrame.\n\t- c: not found in the output DataFrame.\n\t- d: not found in the output DataFrame.
      """)
end
