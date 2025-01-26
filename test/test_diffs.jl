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
    
    @test "@select removed: [\"label\"] " ==
    TidierData.generate_log(test_df, remove_col, "@select", [:colchange])
    @test "@mutate added: [\"num2\"] " ==
    TidierData.generate_log(test_df, add_col, "@mutate", [:colchange])
    @test "@filter removed 2 rows. " ==
    TidierData.generate_log(test_df, remove_rows, "@filter", [:rowchange])
    @test "@transmute removed: [\"name\", \"num\"] " ==
    TidierData.generate_log(test_df, transmute_col, "@transmute", [:colchange])
    @test "@transmute removed: [\"label\", \"name\", \"num\"] @transmute added: [\"labelstring\"] " == TidierData.generate_log(test_df, transmute_col2, "@transmute", [:colchange])
    @test "@transmute removed: [\"label\"] @transmute added: [\"l\"] " ==
    TidierData.generate_log(test_df, rename_col, "@transmute", [:colchange])
    @test "@summarize returned a DataFrame (1 row, 1 column). " ==
    TidierData.generate_log(test_df, summarize_col, "@summarize", [:newsize])
    @test "@group_by added groups: [\"label\"]" ==
    TidierData.generate_log(test_df, group_col, "@group_by", [:groups])
    @test "@ungroup removed groups: [\"label\"]" ==
    TidierData.generate_log(group_col, ungroup, "@ungroup", [:groups])
    @test "@distinct removed 4 rows. " ==
    TidierData.generate_log(double, half, "@distinct", [:rowchange])
    df1 = DataFrame(a = ["a", "b"], b = 1:2); df2 = DataFrame(a = ["a", "c"], c = 3:4);
    @test """@left_join: added 1 new column(s): [\"c\"].\n- Dimension Change: 2×2 -> 2×3\n""" == 
    TidierData.log_join_changes(df1, @left_join(df1, df2), join_type = "@left_join")
    @test !isempty(@chain test_df @mutate( num2 = [2, 3, missing, 5], num5 = [5, 6, missing, 8])  @mutate( num2 = replace_missing(num2, 8)))
end
