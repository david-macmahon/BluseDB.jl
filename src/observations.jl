# Struct and funcs for "observations" table

using Dates

@kwdef mutable struct Observation
  id::Int32 = 0
  start::DateTime
  imjd::Int32
  smjd::Int32
  ra::Float64
  decl::Float64
  src_name::String
  fecenter::Float64
  fenchan::Int32
  nants::Int32
end

const ObservationSelectByIdSQL = """
select
  `start`, `imjd`, `smjd`,
  `ra`, `decl`, `src_name`,
  `fecenter`, `fenchan`, `nants`
from observations
where id=?
"""

function observation_by_id(conn::DBInterface.Connection, id::Integer)::Observation
  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :ObservationSelectByIdSQL)
  cursor = DBInterface.execute(stmt, Int32(id))
  length(cursor) == 0 && error("no observation with id=$id")
  Observation(id, first(cursor)...)
end

const ObservationSelectByUniqueSQL = """
select id, start, src_name from observations where
  `imjd`=? and `smjd`=? and
  `ra`=? and `decl`=? and
  `fecenter`=? and `fenchan`=? and `nants`=?
"""

function unique_values(obs::Observation)
  (
    obs.imjd, obs.smjd,
    obs.ra, obs.decl,
    obs.fecenter, obs.fenchan, obs.nants
  )
end

function select_by_unique!(conn::DBInterface.Connection, obs::Observation)::Bool
  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :ObservationSelectByUniqueSQL)
  cursor = DBInterface.execute(stmt, unique_values(obs))
  length(cursor) == 0 && return false
  row = first(cursor)
  foreach(pairs(row)) do (k,v)
    if k != :id && getfield(obs, k) != v
      @debug """overwriting local "$k" value "$(getfield(obs,k))" with database value "$(v)\""""
    end
    setfield!(obs, k, v)
  end
  true
end

const ObservationInsertSQL = """
insert into observations (
  `id`,
  `start`, `imjd`, `smjd`,
  `ra`, `decl`, `src_name`,
  `fecenter`, `fenchan`, `nants`
) values (
  ?,
  ?, ?, ?,
  ?, ?, ?,
  ?, ?, ?
)
"""

function insert_values(obs::Observation)
  (
    obs.id,
    obs.start, obs.imjd, obs.smjd,
    obs.ra, obs.decl, obs.src_name,
    obs.fecenter, obs.fenchan, obs.nants
  )
end

function insert!(conn::DBInterface.Connection, obs::Observation)::Observation
  # Cannot insert record if id is already non-zero
  @assert obs.id == 0

  # First try to select observation based on unique index
  if !select_by_unique!(conn, obs)
    # Get prepared insert statement from lazily initialized cache
    stmt = prepare(conn, :ObservationInsertSQL)
    try
      cursor = DBInterface.execute(stmt, insert_values(obs))
      # Store the assigned id
      obs.id = DBInterface.lastrowid(cursor)
    catch
      # Assume that exception is unique constraint violation because someone has
      # already inserted the record.
      # TODO Verify that exception is unique constraint violation
      if !select_by_unique!(conn, obs)
        rethrow()
      end
    end
  end
  obs
end
