# Struct and funcs for "rawfiles" table

@kwdef mutable struct RawFile
  id::Int = 0
  observation_id::Int
  obsfreq::Float64
  obsbw::Float64
  nchan::Int
  host::String
  dir::String
  file::String
end

const RawFileSelectByIdSQL = """
select
  `observation_id`,
  `obsfreq`, `obsbw`, `nchan`,
  `host`, `dir`, `file`
from rawfiles
where id=?
"""

function rawfile_by_id(conn, id::Integer)::RawFile
  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :RawFileSelectByIdSQL)
  cursor = DBInterface.execute(stmt, Int64(id))
  length(cursor) == 0 && error("no rawfile with id=$id")
  RawFile(id, first(cursor)...)
end

const RawFileSelectIdSQL = """
select id from rawfiles where
  `host`=? and `dir`=? and `file`=?
"""

function select_id_values(rf::RawFile)
  (
    rf.host, rf.dir, rf.file
  )
end

function select_id!(conn, rf::RawFile)
  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :RawFileSelectIdSQL)
  cursor = DBInterface.execute(stmt, select_id_values(rf))
  rf.id = first(cursor).id
end

const RawFileInsertSQL = """
insert into rawfiles (
  `id`, `observation_id`,
  `obsfreq`, `obsbw`, `nchan`,
  `host`, `dir`, `file`
) values (
  ?, ?,
  ?, ?, ?,
  ?, ?, ?
)
"""

function insert_values(rawfile::RawFile)
  (
    rawfile.id, rawfile.observation.id,
    rawfile.obsfreq, rawfile.obsbw, rawfile.nchan,
    rawfile.host, rawfile.dir, rawfile.file
  )
end

function insert!(conn, rf::RawFile)
  # Cannot insert record if id is already non-zero
  @assert rf.id == 0

  # Insert observation if needed
  if rf.observation.id == 0
    insert!(conn, rf.observation)
  end

  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :RawFileInsertSQL)
  try
    cursor = DBInterface.execute(stmt, insert_values(rf))
    # Store the assigned id
    rf.id = DBInterface.lastrowid(cursor)
  catch
    # Assume that exception is unique constraint violation because someone has
    # already inserted the record.
    # TODO Verify that exception is unique constraint violation
    select_id!(conn, rf)
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
