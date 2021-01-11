module BluseDB

#import Pkg
import YAML

load_credentials() = YAML.load_file(joinpath(ENV["HOME"], ".blusedb.yml"), dicttype=Dict{Symbol,Any})
load_schema() = YAML.load_file(joinpath(@__DIR__, "schema.yml"))

end # module
