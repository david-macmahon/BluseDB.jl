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

const ObservationIdByUniqueSQL = """
select id from observations where
  `imjd`=? and `smjd`=? and
  `ra`=? and `decl`=? and `src_name`=? and
  `fecenter`=? and `fenchan`=? and `nants`=?
"""

const ObservationSelectByUniqueSQL = """
select id, start from observations where
  `imjd`=? and `smjd`=? and
  `ra`=? and `decl`=? and `src_name`=? and
  `fecenter`=? and `fenchan`=? and `nants`=?
"""

function unique_values(obs::Observation)
  (
    obs.imjd, obs.smjd,
    obs.ra, obs.decl, obs.src_name,
    obs.fecenter, obs.fenchan, obs.nants
  )
end

function id_by_unique(conn::DBInterface.Connection, obs::Observation)::Int32
  # Get prepared statement from lazily initialized cache
  stmt = prepare(conn, :ObservationIdByUniqueSQL)
  cursor = DBInterface.execute(stmt, unique_values(obs))
  length(cursor) == 0 ? 0 : first(cursor)[:id]
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

const ObservationUpdateByIdSQL = """
update observations
set start=?, imjd=?, smjd=?,
    ra=?, decl=?, src_name=?,
    fecenter=?, fenchan=?, nants=?
where id=?
"""

function update_by_id_values(obs::Observation)
  (
    obs.start, obs.imjd, obs.smjd,
    obs.ra, obs.decl, obs.src_name,
    obs.fecenter, obs.fenchan, obs.nants,
    obs.id
  )
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

"""
Store `obs` in the database.  If the database has a record for `obs` (based on
the unique constraint), update it with the values from  `obs`.  Otherwise,
insert a new record for `obs`.  If the insert fails because someone else beats
us to it, then update the record added by them with values from `obs`.  Upon
return, the `id` field of `obs` will reflect the value in the database.
"""
function store!(conn::DBInterface.Connection, obs::Observation)::Observation
  # If observation does not yet exist
  obs.id = id_by_unique(conn, obs)
  if obs.id == 0
    # Get prepared insert statement from lazily initialized cache
    stmt = prepare(conn, :ObservationInsertSQL)
    try
      cursor = DBInterface.execute(stmt, insert_values(obs))
      # Store the assigned id
      obs.id = DBInterface.lastrowid(cursor)
      # Done
      return obs
    catch
      # Assume that exception is unique constraint violation because someone has
      # already inserted the record.
      # TODO Verify that exception is unique constraint violation

      # Get id from database so we can update record by id
      obs.id = id_by_unique(conn, obs)
      if obs.id == 0
        rethrow()
      end
    end
  end

  # If we get here, the record needs to be updated
  @assert obs.id != 0

  # Get prepared insert statement from lazily initialized cache
  stmt = prepare(conn, :ObservationUpdateByIdSQL)
  DBInterface.execute(stmt, update_by_id_values(obs))

  obs
end
