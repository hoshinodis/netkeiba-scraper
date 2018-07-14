package netkeibascraper.raceresult

import scalikejdbc._

object RaceResultDao {

  def createTable()(implicit s: DBSession): Boolean = {
    sql"""
create table if not exists race_result (
  race_id int not null,

  order_of_finish    text    not null,
  frame_number       int     not null,
  horse_number       int     not null,
  horse_id           text    not null,
  sex                text    not null,
  age                int     not null,
  basis_weight       real    not null,
  jockey_id          text    not null,
  finishing_time     text    not null,
  length             text    not null,
  speed_figure       int,
  pass               text    not null,
  last_phase         real,
  odds               real,
  popularity         int,
  horse_weight       text    not null,
  remark             text,
  stable             text    not null,
  trainer_id         text    not null,
  owner_id           text    not null,
  earning_money      real,
  primary key (race_id, horse_number),
  foreign key (race_id) references race_info (id)
);""".execute.apply

/*
    sql"""
create index
  race_id_idx
on
  race_result (race_id);
""".execute.apply

    sql"""
create index
  race_id_horse_id_idx
on
  race_result (race_id, horse_id);
""".execute.apply

    sql"""
create index
  race_id_jockey_id_idx
on
  race_result (race_id, jockey_id);
""".execute.apply

    sql"""
create index
  race_id_trainer_id_idx
on
  race_result (race_id, trainer_id);
""".execute.apply

    sql"""
create index
  race_id_owner_id_idx
on
  race_result (race_id, owner_id);
""".execute.apply
*/
  }

  def insert(rr: RaceResult)(implicit s: DBSession): Int = {
    sql"""
replace into race_result (
  race_id,

  order_of_finish,
  frame_number,
  horse_number,
  horse_id,
  sex,
  age,
  basis_weight,
  jockey_id,
  finishing_time,
  length,
  speed_figure,
  pass,
  last_phase,
  odds,
  popularity,
  horse_weight,
  remark,
  stable,
  trainer_id,
  owner_id,
  earning_money
) values (
  ${rr.race_id},

  ${rr.order_of_finish},
  ${rr.frame_number},
  ${rr.horse_number},
  ${rr.horse_id},
  ${rr.sex},
  ${rr.age},
  ${rr.basis_weight},
  ${rr.jockey_id},
  ${rr.finishing_time},
  ${rr.length},
  ${rr.speed_figure},
  ${rr.pass},
  ${rr.last_phase},
  ${rr.odds},
  ${rr.popularity},
  ${rr.horse_weight},
  ${rr.remark},
  ${rr.stable},
  ${rr.trainer_id},
  ${rr.owner_id},
  ${rr.earning_money}
)
""".update.apply()
  }

}
