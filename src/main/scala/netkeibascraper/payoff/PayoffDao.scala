package netkeibascraper.payoff

import scalikejdbc._

object PayoffDao {

  def createTable()(implicit s: DBSession): Boolean = {
    sql"""
create table if not exists payoff (
  race_id      int     not null,
  ticket_type  int     not null check(ticket_type between 0 and 7),
  horse_number varchar(30)     not null,
  payoff       real    not null check(payoff >= 0),
  popularity   int     not null check(popularity >= 0),
  primary key (race_id, ticket_type, horse_number),
  foreign key (race_id) references race_info (id)
)
""".execute.apply
  }

  def insert(dto: Payoff)(implicit s: DBSession): Int = {
    sql"""
replace into payoff (
  race_id,
  ticket_type,
  horse_number,
  payoff,
  popularity
) values (
  ${dto.race_id},
  ${dto.ticket_type},
  ${dto.horse_number},
  ${dto.payoff},
  ${dto.popularity}
)
""".update.apply()
  }

}
