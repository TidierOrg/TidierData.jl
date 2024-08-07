@testset "@slice()" verbose = true begin

@testset "empty slice drops all rows" begin
    df = DataFrame(g = [1, 1, 1], x = 1:3)
    gdf = @group_by(df, g)

    empty_df = DataFrame(g = Int[], x = Int[])
    empty_gdf = @group_by(empty_df, g)

    @test isequal(@slice(df), empty_df)
    @test isequal(@slice(gdf), empty_gdf)
end

@testset "slicing DataFrame yields DataFrame" begin
    df = DataFrame(x = 1:3)
    @test isequal(@slice(df, 1), DataFrame(x = 1))
end

@testset "slice keeps positive indices, ignoring out of range" begin
    gf = @group_by(DataFrame(g = [1, 2, 2, 3, 3, 3], id = 1:6), g)

    #=
    grouped dataframes don't behave exactly the same in Julia,
    so you can't directly index into them like you can in R.
    this feels like a close enough approximation of this test though.
    =#
    out = @slice(gf, 1)
    @test isequal(@ungroup(out)[!, :id], [1, 2, 4])

    out = @slice(gf, 2)
    @test isequal(@ungroup(out)[!, :id], [3, 5])
end

@testset "slice keeps negative indices, ignoring out of range" begin
    gf = @group_by(DataFrame(g = [1, 2, 2, 3, 3, 3], id = 1:6), g)

    out = @slice(gf, -1)
    @test isequal(@ungroup(out)[!, :id], [3, 5, 6])

    out = @slice(gf, -(1:2))
    @test isequal(@ungroup(out)[!, :id], 6)
end

@testset "slice errors if positive and negative indices mixed" begin
    @test_throws "@slice() indices must either be all positive or all negative." @slice(DataFrame(), 1, -1)
end

@testset "slice errors if index is not numeric" begin
    caught_error = false

    try
        eval(quote
            df = DataFrame()
            @slice(df, "a")
        end)
    catch e
        caught_error = true
        @test isa(e, MethodError) || isa(e, LoadError)
    end

    @test caught_error
end

@testset "slice keeps zero length groups" begin
    df = DataFrame(
        e = 1,
        f = categorical([1, 1, 2, 2]; ordered = true, levels = 1:3),
        g = [1, 1, 2, 2],
        x = [1, 2, 1, 4]
    )
    df = @group_by(df, e, f, g)

    @test isequal(
        combine(@slice(df, 1), nrow => :size)[!, :size],
        [1, 1, 0]
    )
end

@testset "slice retains labels for zero length groups" begin
    df = DataFrame(
        e = 1,
        f = categorical([1, 1, 2, 2]; ordered = true, levels = 1:3),
        g = [1, 1, 2, 2],
        x = [1, 2, 1, 4]
    )
    df = @group_by(df, e, f, g)

    @test isequal(
        @ungroup(@count(@slice(df, 1))),
        DataFrame(
            e = 1,
            f = categorical(1:3; ordered = true, levels = 1:3),
            g = [1, 2, missing],
            n = [1, 1, 0]
        )
    )
end
end

#TODO: slice_max tests
#TODO: slice_min tests
