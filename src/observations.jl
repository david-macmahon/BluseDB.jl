# Struct and funcs for "observations" table

using Dates

@kwdef mutable struct Observation
  id::Int = 0
  start::DateTime
  imjd::Int
  smjd::Int
  ra::Float64
  decl::Float64
  src_name::String
  fecenter::Float64
  fenchan::Int
  nants::Int
  dwell::Float64
end

const ObservationSelectByIdSQL = """
select
  `start`, `imjd`, `smjd`,
  `ra`, `decl`, `src_name`,
  `fecenter`, `fenchan`, `nants`,
  `dwell`
from observations
where id=?
"""

function observation_by_id(conn, id::Integer)::Observation
  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :ObservationSelectByIdSQL)
  cursor = DBInterface.execute(stmt, Int64(id))
  length(cursor) == 0 && error("no observation with id=$id")
  Observation(id, first(cursor)...)
end

const ObservationSelectIdSQL = """
select id from observations where
  `imjd`=? and `smjd`=? and
  `ra`=? and `decl`=? and
  `fecenter`=? and `fenchan`=? and `nants`=?
"""

function select_id_values(obs::Observation)
  (
    obs.imjd, obs.smjd,
    obs.ra, obs.decl,
    obs.fecenter, obs.fenchan, obs.nants
  )
end

function select_id!(conn, obs)
  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :ObservationSelectIdSQL)
  cursor = DBInterface.execute(stmt, select_id_values(obs))
  obs.id = first(cursor).id
end

const ObservationInsertSQL = """
insert into observations (
  `id`,
  `start`, `imjd`, `smjd`,
  `ra`, `decl`, `src_name`,
  `fecenter`, `fenchan`, `nants`,
  `dwell`
) values (
  ?,
  ?, ?, ?,
  ?, ?, ?,
  ?, ?, ?,
  ?
)
"""

function insert_values(obs::Observation)
  (
    obs.id,
    obs.start, obs.imjd, obs.smjd,
    obs.ra, obs.decl, obs.src_name,
    obs.fecenter, obs.fenchan, obs.nants,
    obs.dwell
  )
end

function insert!(conn, obs::Observation)
  # Cannot insert record if id is already non-zero
  @assert obs.id == 0

  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :ObservationInsertSQL)
  try
    cursor = DBInterface.execute(stmt, insert_values(obs))
    # Store the assigned id
    obs.id = DBInterface.lastrowid(cursor)
  catch
    # Assume that exception is unique constraint violation because someone has
    # already inserted the record.
    # TODO Verify that exception is unique constraint violation
    select_id!(conn, obs)
  end
  obs
end
