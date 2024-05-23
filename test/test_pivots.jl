@testset "pivots" verbose = true begin
@testset "pivot_wider" begin
    true_wide = DataFrame(
            label = [1, 2],
            A = [1, 3],
            B = [2, 4]
        )
    test_wide = @pivot_wider(test_df, names_from="name", values_from="num")
    test_wide2 = @pivot_wider(test_df, names_from=name, values_from=num)
    test_wide3 = @pivot_wider(test_df, names_from=:name, values_from=:num)
    @test all(Array(true_wide .== test_wide))
    @test all(Array(true_wide .== test_wide2))
    @test all(Array(true_wide .== test_wide3))
end

@testset "pivot_longer" begin
    true_long1 = DataFrame(
            label = [1,1,2,2,1,1,2,2],
            variable = ["name","name","name","name","num","num","num","num"],
            value = ["A","B","A","B",1,2,3,4],
        )
    test_long1 = @pivot_longer(test_df, -label)
    test_long2 = @pivot_longer(test_df, name:num)
    
    true_long3 = DataFrame(
        name = ["A","B","A","B"],
        num = [1,2,3,4],
        variable = ["label","label","label","label"],
        value = [1,1,2,2]
    )
    test_long3 = @pivot_longer(test_df, -(name:num))
    test_long4 = @pivot_longer(test_df, label)

    true_long5 = DataFrame(
        name = ["A","B","A","B","A","B","A","B"],
        variable = ["label","label","label","label","num","num","num","num"],
        value = [1,1,2,2,1,2,3,4],
    )
    test_long5 = @pivot_longer(test_df, [label,num])
    
    true_long6 = DataFrame(
        label = [1,1,2,2],
        num = [1,2,3,4],
        variable = ["name","name","name","name"],
        value = ["A","B","A","B"],
    )
    test_long6 = @pivot_longer(test_df, -[label,num])

    @test all(Array(true_long1 .== test_long1))
    @test all(Array(true_long1 .== test_long2))
    @test all(Array(true_long3 .== test_long3))
    @test all(Array(true_long3 .== test_long4))
    @test all(Array(true_long5 .== test_long5))
    @test all(Array(true_long6 .== test_long6))
end
end
