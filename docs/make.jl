using VMPerformance
using Documenter

DocMeta.setdocmeta!(VMPerformance, :DocTestSetup, :(using VMPerformance); recursive=true)

makedocs(;
    modules=[VMPerformance],
    authors="Kobus Herbst",
    sitename="VMPerformance.jl",
    format=Documenter.HTML(;
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)
