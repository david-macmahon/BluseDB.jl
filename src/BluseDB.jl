module BluseDB

using MySQL
import YAML
import Base: @kwdef

@enum UserLevel admin user reader

const STATEMENT_CACHE = Dict{Tuple{DBInterface.Connection, Symbol},
                             DBInterface.Statement}()

function prepare(conn::DBInterface.Connection,
                 key::Symbol)::DBInterface.Statement
  haskey(STATEMENT_CACHE, (conn, key)) ?
    STATEMENT_CACHE[(conn, key)] :
    DBInterface.prepare(conn, getfield(@__MODULE__, key))
end

function unprepare(conn::DBInterface.Connection)
  goners = findall(cs->cs[1]==conn, keys(STATEMENT_CACHE))
  for goner in goners
    stmt = STATEMENT_CACHE[goner]
    delete!(STATEMENT_CACHE, goner)
    DBInterface.close(stmt)
  end
  nothing
end

function load_credentials()
  credfile = joinpath(ENV["HOME"], ".blusedb.yml")
  YAML.load_file(credfile, dicttype=Dict{Symbol,Any})
end

function load_credentials(userlevel::UserLevel)
  creds = load_credentials()
  if haskey(creds, Symbol(userlevel))
    creds = merge(creds, creds[Symbol(userlevel)])
  end
  creds
end

load_schema() = YAML.load_file(joinpath(@__DIR__, "schema.yml"))

function connect(userlevel::UserLevel; kwargs...)::DBInterface.Connection
  creds = load_credentials(userlevel)

  host = get(creds, :host, "localhost")
  username = get(creds, :username, "")
  password = get(creds, :password, "")
  database = get(creds, :database, "blusedb")

  @info "connecting as $username/$password@$host using database $database"
  conn = DBInterface.connect(MySQL.Connection,
                             host, username, password; db=database, kwargs...)
end

function create_schema()
  conn = connect(admin)
  try
    schema = load_schema()
    for sql in schema
      DBInterface.execute(conn, sql)
    end
  finally
    DBInterface.close!(conn)
  end
  nothing
end

include("observations.jl")
include("rawfiles.jl")

end # module
