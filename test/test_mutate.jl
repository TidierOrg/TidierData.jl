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

    @testset " length-1 vectors are recycled" begin
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
                z = 1,
                z = nothing
            end
            ),
            df
        )
    end
end
