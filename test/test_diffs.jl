@testset "diffs" begin
    remove_col = @select(test_df, name, num)
    add_col = @mutate(test_df, num2 = [2, 3, 4, 5])
    remove_rows = @filter(test_df, name == "A")

    @test_logs (:info, "@select removed: [\"label\"]")
    TidierData.generate_log(test_df, remove_col, "@select", [:colchange])
    @test_logs (:info, "@mutate added: [\"num2\"]")
    TidierData.generate_log(test_df, add_col, "@mutate", [:colchange])
    @test_logs (:info, "@filter removed 2 rows.")
    TidierData.generate_log(test_df, remove_rows, "@filter", [:rowchange])
end
