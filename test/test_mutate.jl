@testset "mutate" verbose = true begin

    @testset "empty mutate returns input" begin
        df = DataFrame(x = 1)
        gf = @group_by(df, x)

        @test isequal(@mutate(df), df)
        @test isequal(@mutate(gf), gf)


        @test isequal(@mutate(df, []), df)
        @test isequal(@mutate(gf, []), gf)
    end

    @testset "mutations applied progressively" begin
        df = DataFrame(x = 1)
        @test isequal(
            (@mutate df begin
                    y = x + 1
                    z = y + 1
                end
            ),
            DataFrame(x = 1, y = 2, z = 3)
        )

        @test isequal(
            (@mutate df begin
                    x = x + 1
                    x = x + 1
                end
            ),
            DataFrame(x = 3)
        )

        @test isequal(
            (@mutate df begin
                    x = 2
                    y = x
                end
            ),
            DataFrame(x = 2, y = 2)
        )

        df = DataFrame(x = 1, y = 2)
        @test isequal(
            (@mutate df begin
                x2 = x
                x3 = x2 + 1
            end
            ),
            (@mutate df begin
                x2 = x + 0
                x3 = x2 + 1
            end
            )
        )
    end

    @testset "length-1 vectors are recycled" begin
        df = DataFrame(x = 1:4)
        @test isequal(@mutate(df, y = 1)[!, :y], fill(1, 4))
        @test_throws "ArgumentError: New columns must have the same length as old columns" @mutate(df, y = 1:2)
    end

    @testset "can remove variables with nothing" begin
        df = DataFrame(x = 1:3, y = 1:3)

        @test isequal(@mutate(df, y = nothing), df[:, [1]])
        @test isequal(@ungroup(@mutate(gf, y = nothing)), gf[:, [1]])

        # even if it doesn't exist
        @test isequal(@mutate(df, z = nothing), df[:, [1]])

        # or was just created
        @test isequal(
            (@mutate df begin
                z = 1
                z = nothing
            end
            ),
            df
        )
    end

    @testset "mutate supports constants" begin
        df = DataFrame(x = 1:10, g =  repeat(1:2, inner = 5))
        y = 1:10
        z = 1:5

        @test isequal(@mutate(df, y = !!y)[!, :y], y)
        #=
            i'm not actually sure whether this test would be one-to-one with Julia,
            given that grouped dataframes behave differently than in R.
        =#
        # @test isequal(
        #     (@chain df begin
        #         @group_by(g)
        #         @mutate y = !!y
        #         @ungroup
        #         @pull y
        #     end
        #     )
        # )
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
