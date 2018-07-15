package netkeibascraper.raceinfo

import scalikejdbc._

object RaceInfoDao {

  def createTable()(implicit s: DBSession): Boolean = {
    sql"""create table if not exists race_info (
  id int auto_increment not null primary key,

  race_name     text    not null,
  surface       text    not null,
  distance      int     not null,
  weather       text    not null,
  surface_state text    not null,

  race_start    text    not null,
  race_number   int     not null,

  surface_score int,
  date          text    not null,
  place_detail  text    not null,
  race_class    text    not null
);""".execute.apply
  }

  def createIndex()(implicit s: DBSession): Boolean = {

    sql"""
create index
  date_idx
on
  race_info (date);
""".execute.apply

    sql"""
create index
  id_date_idx
on
  race_info (id, date);
""".execute.apply

  }

  def insert(ri: RaceInfo)(implicit s: DBSession): Int = {
    sql"""
replace into race_info (
  race_name,
  surface,
  distance,
  weather,
  surface_state,
  race_start,
  race_number,
  surface_score,
  date,
  place_detail,
  race_class
) values (
  ${ri.race_name},
  ${ri.surface},
  ${ri.distance},
  ${ri.weather},
  ${ri.surface_state},
  ${ri.race_start},
  ${ri.race_number},
  ${ri.surface_score},
  ${ri.date},
  ${ri.place_detail},
  ${ri.race_class}
);
""".update.apply()
  }

  def lastRowId()(implicit s: DBSession): Int = {
    sql"""
select last_insert_id() as last_rowid
""".map(_.int("last_rowid")).single.apply().get
  }

  def getById(id: Int)(implicit s: DBSession): RaceInfo = {
    sql"""select * from race_info where id = ${id}""".
      map{ x =>
        RaceInfo(
          race_name = x.string("race_name"),
          surface = x.string("surface"),
          distance = x.int("distance"),
          weather = x.string("weather"),
          surface_state = x.string("surface_state"),
          race_start = x.string("race_start"),
          race_number = x.int("race_number"),
          surface_score = x.intOpt("surface_score"),
          date = x.string("date"),
          place_detail = x.string("place_detail"),
          race_class = x.string("race_class")
        )
      }.single.apply().get
  }

}
