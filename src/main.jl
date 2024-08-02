using VMPerformance
using PackageCompiler

#@time clean_education()
package_dir = "E:\\Source\\VMPerformance.jl"
compiled_app = "E:\\Source\\VMPerformance.jl\\compiled"
create_app(package_dir, compiled_app; force=true)