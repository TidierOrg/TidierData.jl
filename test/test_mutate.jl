@testset "@mutate()" verbose = true begin

    @testset "empty mutate returns input" begin
        df = DataFrame(x = 1)
        gf = @group_by(df, x)

        @test isequal(@mutate(df), df)
        @test isequal(@mutate(gf), gf)


        @test isequal(@mutate(df, []), df)
        @test isequal(@mutate(gf, []), gf)
    end

    @testset "length-1 vectors are recycled" begin
        df = DataFrame(x = 1:4)
        @test isequal(@mutate(df, y = 1)[!, :y], fill(1, 4))
        @test_throws "ArgumentError: New columns must have the same length as old columns" @mutate(df, y = 1:2)
    end

    @testset "mutate supports constants" begin
        df = DataFrame(x = 1:10, g =  repeat(1:2, inner = 5))
        y = 1:10
        z = 1:5

        @test isequal(@mutate(df, y = !!y)[!, :y], y)
    end

    @testset "mutate works on empty dataframes" begin
        df = DataFrame()
        res = @mutate(df)
        @test isequal(nrow(res), 0)
        @test isequal(ncol(res), 0)

        res = @mutate(df, x = Int64[])
        @test isequal(names(res), ["x"])
        @test isequal(nrow(res), 0)
        @test isequal(ncol(res), 1)
    end

end
