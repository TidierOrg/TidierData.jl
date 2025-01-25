@testset "diffs" begin
    remove_col = @select(test_df, name, num)
    add_col = @mutate(test_df, num2 = [2, 3, 4, 5])
    remove_rows = @filter(test_df, name == "A")

    @test_logs (:info, "Removed: [\"label\"]")
    TidierData.generate_log(test_df, remove_col)
    @test_logs (:info, "Added: [\"num2\"]")
    TidierData.generate_log(test_df, add_col)
    @test_logs (:info, "Changed: 3 rows")
    TidierData.generate_log(test_df, remove_rows)
end
