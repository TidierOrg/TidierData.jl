module TestTidierData

using TidierData
using Test
using Documenter

DocMeta.setdocmeta!(TidierData, :DocTestSetup, :(using TidierData); recursive=true)

doctest(TidierData)

end