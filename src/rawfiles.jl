# Struct and funcs for "rawfiles" table

@kwdef mutable struct RawFile
  id::Int32 = 0
  observation_id::Int32
  obsfreq::Float64
  obsbw::Float64
  nchan::Int32
  host::String
  dir::String
  file::String
  lastseen::DateTime
end

const RawFileSelectByIdSQL = """
select
  `observation_id`,
  `obsfreq`, `obsbw`, `nchan`,
  `host`, `dir`, `file`, `lastseen`
from rawfiles
where id=?
"""

function rawfile_by_id(conn::DBInterface.Connection, id::Integer)::RawFile
  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :RawFileSelectByIdSQL)
  cursor = DBInterface.execute(stmt, Int32(id))
  length(cursor) == 0 && error("no rawfile with id=$id")
  RawFile(id, first(cursor)...)
end

const RawFileSelectByUniqueSQL = """
select id, observation_id, obsfreq, obsbw, nchan, lastseen
from rawfiles
where `host`=? and `dir`=? and `file`=?
"""

function unique_values(rf::RawFile)
  (
    rf.host, rf.dir, rf.file
  )
end

function select_by_unique!(conn::DBInterface.Connection, rf::RawFile)::Bool
  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :RawFileSelectByUniqueSQL)
  cursor = DBInterface.execute(stmt, unique_values(rf))
  length(cursor) == 0 && return false
  row = first(cursor)
  foreach(pairs(row)) do (k,v)
    if k != :id && getfield(rf, k) != v
      @debug """overwriting local "$k" value "$(getfield(rf,k))" with database value "$(v)\""""
    end
    setfield!(rf, k, v)
  end
  true
end

const RawFileInsertSQL = """
insert into rawfiles (
  `id`, `observation_id`,
  `obsfreq`, `obsbw`, `nchan`,
  `host`, `dir`, `file`, `lastseen`
) values (
  ?, ?,
  ?, ?, ?,
  ?, ?, ?, ?
)
"""

function insert_values(rawfile::RawFile)
  (
    rawfile.id, rawfile.observation_id,
    rawfile.obsfreq, rawfile.obsbw, rawfile.nchan,
    rawfile.host, rawfile.dir, rawfile.file, rawfile.lastseen
  )
end

function insert!(conn::DBInterface.Connection, rf::RawFile)::RawFile
  # Cannot insert record if id is already non-zero
  @assert rf.id == 0

  # Cannot insert record if observation_id is zero
  @assert rf.observation_id != 0

  # First try to select rawfile based on unique index
  if !select_by_unique!(conn, rf)
    # Get prepared insert statement from lazily initialized cache
    stmt = prepare(conn, :RawFileInsertSQL)
    try
      cursor = DBInterface.execute(stmt, insert_values(rf))
      # Store the assigned id
      rf.id = DBInterface.lastrowid(cursor)
    catch
      # Assume that exception is unique constraint violation because someone has
      # already inserted the record.
      # TODO Verify that exception is unique constraint violation
      if !select_by_unique!(conn, rf)
        rethrow()
      end
    end
  end
  rf
end

# Provide custom Tables.columnnames() to shorten `observation_id` to `obsid`
import Tables

function Tables.getcolumn(rf::RawFile, nm::Symbol)
  (nm == :obsid) && (nm = :observation_id)
  getproperty(rf, nm)
end

function Tables.columnnames(rf::RawFile)
  props = propertynames(rf)
  (props[1], :obsid, props[3:end]...)
end
