using Documenter, DocumenterMarkdown
using TidierData, DataFrames, RDatasets

DocTestMeta = quote
    using TidierData, DataFrames, Chain, Statistics
end
DocMeta.setdocmeta!(TidierData,
    :DocTestSetup,
    DocTestMeta;
    recursive=true)
makedocs(
    modules=[TidierData],
    clean=true,
    doctest=true,
    #format   = Documenter.HTML(prettyurls = get(ENV, "CI", nothing) == "true"),
    sitename="TidierData.jl",
    authors="Karandeep Singh et al.",
    strict=[
        :doctest,
        :linkcheck,
        :parse_error,
        :example_block,
        # Other available options are
        # :autodocs_block, :cross_references, :docs_block, :eval_block, :example_block,
        # :footnote, :meta_block, :missing_docs, :setup_block
    ], checkdocs=:all, format=Markdown(), draft=false,
    build=joinpath(@__DIR__, "docs")
)

deploydocs(; devurl="latest", repo="github.com/TidierOrg/TidierData.jl", push_preview=true,
    deps=Deps.pip("mkdocs", "pygments", "python-markdown-math", "mkdocs-material",
        "pymdown-extensions", "mkdocstrings", "mknotebooks",
        "pytkdocs_tweaks", "mkdocs_include_exclude_files", "jinja2", "mkdocs-video"),
    make=() -> run(`mkdocs build`), target="site", devbranch="main")
