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

const RawFileIdByUniqueSQL = """
select id
from rawfiles
where `host`=? and `dir`=? and `file`=?
"""

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

function id_by_unique(conn::DBInterface.Connection, rf::RawFile)::Int32
  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :RawFileIdByUniqueSQL)
  cursor = DBInterface.execute(stmt, unique_values(rf))
  length(cursor) == 0 ? 0 : first(cursor)[:id]
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

const RawFileUpdateByIdSQL = """
update rawfiles
set observation_id=?, obsfreq=?, obsbw=?, nchan=?,
    host=?, dir=?, file=?, lastseen=?
where id=?
"""

function update_by_id_values(rf::RawFile)
  (
    rf.observation_id, rf.obsfreq, rf.obsbw, rf.nchan,
    rf.host, rf.dir, rf.file, rf.lastseen, rf.id
  )
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

function insert_values(rf::RawFile)
  (
    rf.id, rf.observation_id,
    rf.obsfreq, rf.obsbw, rf.nchan,
    rf.host, rf.dir, rf.file, rf.lastseen
  )
end

"""
Store `rf` in the database.  If the database has a record for `rf` (based on
the unique constraint), update it with the values from  `rf`.  Otherwise,
insert a new record for `rf`.  If the insert fails because someone else beats
us to it, then update the record added by them with values from `rf`.  Upon
return, the `id` field of `rf` will reflect the value in the database.
"""
function store!(conn::DBInterface.Connection, rf::RawFile)::RawFile
  # Cannot insert/update record if observation_id is zero
  @assert rf.observation_id != 0

  # If rawfile does not yet exist
  rf.id = id_by_unique(conn, rf)
  if rf.id == 0
    # Get prepared insert statement from lazily initialized cache
    stmt = prepare(conn, :RawFileInsertSQL)
    try
      cursor = DBInterface.execute(stmt, insert_values(rf))
      # Store the assigned id
      rf.id = DBInterface.lastrowid(cursor)
      # Done
      return rf
    catch
      # Assume that exception is unique constraint violation because someone has
      # already inserted the record.
      # TODO Verify that exception is unique constraint violation

      # Get id from database so we can update record by id
      rf.id = id_by_unique!(conn, rf)
      if rf.id == 0
        rethrow()
      end
    end
  end

  # If we get here, the record needs to be updated
  @assert rf.id != 0

  # Get prepared insert statement from lazily initialized cache
  stmt = prepare(conn, :RawFileUpdateByIdSQL)
  DBInterface.execute(stmt, update_by_id_values(rf))

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
