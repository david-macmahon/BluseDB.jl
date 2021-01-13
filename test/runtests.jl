using Test
using BluseDB
using MySQL
import Dates

# Ensure BLUSEDB_ENV is not in ENV
delete!(ENV, "BLUSEDB_ENV")
@test ~haskey(ENV, "BLUSEDB_ENV")

@test_throws AssertionError BluseDB.connect(BluseDB.admin)

# Use the "test" environment
ENV["BLUSEDB_ENV"] = "test"

@test nothing === BluseDB.connect(identity, BluseDB.admin)
@test nothing === BluseDB.connect(identity, BluseDB.user)
@test nothing === BluseDB.connect(identity, BluseDB.reader)

# Drop tables
@test nothing === BluseDB.connect(BluseDB.admin) do db
  DBInterface.execute(db, "drop table if exists rawfiles")
  DBInterface.execute(db, "drop table if exists observations")
end

# Create schema
@test nothing === BluseDB.create_schema()

# Connect to database as user
db = BluseDB.connect(BluseDB.user)
@test isa(db, DBInterface.Connection)

# Make some Observation objects
now = trunc(Dates.now(), Dates.Second)
obs1=BluseDB.Observation(id=0, start=now, imjd=1, smjd=2, ra=3, decl=4, src_name="foo", fecenter=5, fenchan=6, nants=7, dwell=8)
obs2=BluseDB.Observation(id=0, start=now, imjd=2, smjd=2, ra=3, decl=4, src_name="foo", fecenter=5, fenchan=6, nants=7, dwell=8)

# Insert obs1
BluseDB.insert!(db, obs1)

# Should get id 1
@test obs1.id == 1

# Try to insert again with non-zero id
@test_throws AssertionError BluseDB.insert!(db, obs1)

# Insert again with zero id
obs1.id = 0
BluseDB.insert!(db, obs1)
# Should get id 1 again
@test obs1.id == 1

# Change fields that are not part of the unique index, try to insert again
# (with zero id), and verify that fields get updated with values from database.
# Need to decide which side wins in a conflict, for now the database side wins.
obs1.id = 0
obs1.start = now - Dates.Second(10)
obs1.src_name = "localname"
obs1.dwell = 3.14
BluseDB.insert!(db, obs1)
# Should get id 1 again
@test obs1.id == 1
@test obs1.start == now
@test obs1.src_name == "foo"
@test obs1.dwell == 8

# Insert obs2
BluseDB.insert!(db, obs2)

# Should get id 2
@test obs2.id == 2

# Make some RawFile objects
rf1a=BluseDB.RawFile(id=0, observation_id=1, obsfreq=1, obsbw=2, nchan=3, host="hosta", dir="dir", file="file");
rf1b=BluseDB.RawFile(id=0, observation_id=1, obsfreq=1, obsbw=2, nchan=3, host="hostb", dir="dir", file="file");
rf2c=BluseDB.RawFile(id=0, observation_id=2, obsfreq=1, obsbw=2, nchan=3, host="hostc", dir="dir", file="file");

# Insert them
BluseDB.insert!(db, rf1a)
@test rf1a.id == 1
BluseDB.insert!(db, rf1b)
@test rf1b.id == 2

# Try to insert again with non-zero id
@test_throws AssertionError BluseDB.insert!(db, rf1a)
# Try to insert again with zero observation_id
rf1a.id = 0
rf1a.observation_id = 0
@test_throws AssertionError BluseDB.insert!(db, rf1a)
# Insert again with zero id and non-zero observation_id
rf1a.observation_id = 1
BluseDB.insert!(db, rf1a)

# Try to insert new record with invalid observation_id
rf2c.observation_id = 999
@test_throws MySQL.API.StmtError BluseDB.insert!(db, rf2c)

# Insert new record again with proper values
rf2c.observation_id = 2
BluseDB.insert!(db, rf2c)
@test rf2c.id == 4 # Failed new insert "wasted" an id value
