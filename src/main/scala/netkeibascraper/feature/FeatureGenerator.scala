package netkeibascraper.feature

import netkeibascraper.raceinfo.{RaceInfo, RaceInfoDao}
import netkeibascraper.util.Util
import scalikejdbc._

import scala.util.Try

class FeatureGenerator(
  val race_id: Int,
  val horse_number: Int
)(implicit s: DBSession) {
  assert(horse_number > 0)

  val RaceInfo(
    race_name,
    rawSurface,
    distance,
    rawWeather,
    surface_state,
    race_start,
    race_number,
    surface_score,
    date,
    place_detail,
    race_class
  ) = RaceInfoDao.getById(race_id)


  val grade: Int = Util.str2cls(race_class)

  val surfaceScore: Option[Int] = surface_score

  val order_of_finish: String =
    sql"""
         |select
         |  order_of_finish
         |from
         |  race_result
         |where
         |  race_id = ${race_id}
         |and
         |  horse_number = ${horse_number}
      """.stripMargin
      .map(_.string("order_of_finish"))
      .single
      .apply()
      .get

  val horse_id: String =
    sql"""
         |select
         |  horse_id
         |from
         |  race_result
         |where
         |  race_id = ${race_id}
         |and
         |  horse_number = ${horse_number}
      """.stripMargin
      .map(_.string("horse_id"))
      .single
      .apply()
      .get

  val jockey_id: String =
    sql"""
         |select
         |  jockey_id
         |from
         |  race_result
         |where
         |  race_id = ${race_id}
         |and
         |  horse_number = ${horse_number}
       """.stripMargin
      .map(_.string("jockey_id"))
      .single
      .apply()
      .get

  val trainer_id: String =
    sql"""
         |select
         |  trainer_id
         |from
         |  race_result
         |where
         |  race_id = ${race_id}
         |and
         |  horse_number = ${horse_number}
       """.stripMargin
      .map(_.string("trainer_id"))
      .single
      .apply()
      .get

  val owner_id: String =
    sql"""
         |select
         |  owner_id
         |from
         |  race_result
         |where
         |  race_id = ${race_id}
         |and
         |  horse_number = ${horse_number}
       """.stripMargin
      .map(_.string("owner_id"))
      .single
      .apply()
      .get

  //Speed rating for the previous race in which the horse ran
  val preSRa: Option[Int] = {
    sql"""
         |select
         |  speed_figure
         |from
         |  race_result
         |inner join
         |  race_info
         |on
         |  race_result.race_id = race_info.id
         |where
         |  horse_id = ${horse_id}
         |and
         |  race_info.date < ${date}
         |and
         |  speed_figure is not null
         |order by date desc
         |limit 1
      """.stripMargin
      .map(_.int("speed_figure"))
      .single
      .apply()
  }

  //The average of a horse’s speed rating in its last 4 races; value of zero when there is no past run
  val avgsr4: Option[Double] = {
    val srs =
      sql"""
           |select
           |  speed_figure
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  horse_id = ${horse_id}
           |and
           |  race_info.date < ${date}
           |and
           |  speed_figure is not null
           |order by date desc
           |limit 4
        """.stripMargin
        .map(_.double("speed_figure"))
        .list
        .apply()

    if (srs.isEmpty)
      None
    else
      Some(srs.sum / srs.size)
  }

  val avgWin4: Option[Double] = {
    val wins =
      sql"""
           |select
           |  (order_of_finish = '1' or order_of_finish = '2' or order_of_finish = '3') as is_win
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  horse_id = ${horse_id}
           |and
           |  race_info.date < ${date}
           |and
           |  speed_figure is not null
           |order by date desc
           |limit 4
        """.stripMargin
        .map(_.double("is_win"))
        .list
        .apply()

    if (wins.isEmpty)
      None
    else
      Some(wins.sum / wins.size)
  }

  //The average speed rating of the past runs of each horse at this distance; value of zero when no previous run
  val disavesr: Option[Double] = {
    val srs =
      sql"""
           |select
           |  speed_figure
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  horse_id = ${horse_id}
           |and
           |  race_info.date < ${date}
           |and
           |  distance = ${distance}
           |and
           |  speed_figure is not null
           |order by date desc
           |limit 100
        """.stripMargin
        .map(_.double("speed_figure"))
        .list
        .apply()

    if (srs.isEmpty)
      None
    else
      Some(srs.sum / srs.size)
  }

  val disRoc: Option[Double] = {
    val distances =
      sql"""
           |select
           |  distance
           |from
           |  race_result
           |inner join
           |  race_info
           |on race_result.race_id = race_info.id
           |where
           |  horse_id = ${horse_id}
           |and
           |  race_info.date < ${date}
           |order by date desc
           |limit 100
        """.stripMargin
        .map(_.double("distance"))
        .list
        .apply()

    if (distances.isEmpty)
      None
    else {
      val mean = distances.sum.toDouble / distances.size
      Some((distance - mean) / mean)
    }
  }

  //Total prize money earnings (finishing first, second or third) to date/Number of races entered
  val eps: Option[Double] = {
    val earning_money =
      sql"""
           |select
           |  earning_money
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  horse_id = ${horse_id}
           |and
           |  race_info.date < ${date}
           |order by date desc
           |limit 100
        """.stripMargin
        .map(_.doubleOpt("earning_money"))
        .list
        .apply()

    if (earning_money.isEmpty)
      None
    else
      Some(earning_money.flatten.sum / earning_money.size)
  }

  //Weight carried by the horse in current race
  val weight: Double = {
    sql"""
         |select
         |  basis_weight
         |from
         |  race_result
         |where
         |  race_id = ${race_id}
         |and
         |  horse_number = ${horse_number}
      """.stripMargin
      .map(_.double("basis_weight"))
      .single
      .apply
      .get
  }

  val hweight: Option[Int] = {
    Try{
      sql"""
           |select
           |  horse_weight
           |from
           |  race_result
           |where
           |  race_id = ${race_id}
           |and
           |  horse_number = ${horse_number}
        """.stripMargin
        .map(_.string("horse_weight"))
        .single
        .apply
        .get
        .replaceAll("""\([^\)]+\)""", "")
        .toInt
    }.toOption
  }

  val dhweight: Option[Int] = {
    Try{
      sql"""
           |select
           |  horse_weight
           |from
           |  race_result
           |where
           |  race_id = ${race_id}
           |and
           |  horse_number = ${horse_number}
        """.stripMargin
        .map(_.string("horse_weight"))
        .single
        .apply
        .get
        .replaceAll(""".*\(([^\)]+)\).*""", "$1")
        .toInt
    }.toOption
  }

  //The percentage of the races won by the horse in its career
  val winRun: Option[Double] = {
    val wins =
      sql"""
           |select
           |  (order_of_finish = '1') as is_win
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  horse_id = ${horse_id}
           |and
           |  race_info.date < ${date}
           |limit 100
        """.stripMargin
        .map(_.int("is_win"))
        .list
        .apply

    if (wins.isEmpty)
      None
    else
      Some(wins.sum.toDouble / wins.size)
  }

  //The winning percentage of the trainer in career to date of race
  val twinper: Option[Double] = {
    val wins =
      sql"""
           |select
           |  (order_of_finish = '1') as is_win
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  trainer_id = ${trainer_id}
           |and
           |  race_info.date < ${date}
           |order by date desc
           |limit 100
        """.stripMargin
        .map(_.int("is_win"))
        .list
        .apply

    if (wins.isEmpty)
      None
    else
      Some(wins.sum.toDouble / wins.size)
  }

  //The winning percentage of the owner in career to date of race
  val owinper: Option[Double] = {
    val wins =
      sql"""
           |select
           |  (order_of_finish = '1') as is_win
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  owner_id = ${owner_id}
           |and
           |  race_info.date < ${date}
           |order by date desc
           |limit 100
        """.stripMargin
        .map(_.int("is_win"))
        .list
        .apply

    if (wins.isEmpty)
      None
    else
      Some(wins.sum.toDouble / wins.size)
  }

  //The winning percentage of the jockey in career to date of race
  val age: Double = {
    sql"""
         |select
         |  age
         |from
         |  race_result
         |where
         |  race_id = ${race_id}
         |and
         |  horse_number = ${horse_number}
      """.stripMargin
      .map(_.double("age"))
      .single
      .apply
      .get
  }

  val dsl: Option[Double] = {
    sql"""
         |select
         |  (cast(${date} as date) - cast(date as date)) as dsl
         |from
         |  race_result
         |inner join
         |  race_info
         |on
         |  race_result.race_id = race_info.id
         |where
         |  horse_id = ${horse_id}
         |and
         |  race_info.date < ${date}
         |and
         |  speed_figure is not null
         |order by date desc
         |limit 1
      """.stripMargin
      .map(_.double("dsl"))
      .single
      .apply()
  }

  val surface: String = {
    Util.surface(rawSurface)
  }

  val weather: String = {
    Util.weather(rawWeather)
  }

  val sex: String = {
    val state =
      sql"""
           |select
           |  sex
           |from
           |  race_result
           |where
           |  race_id = ${race_id}
           |and
           |  horse_number = ${horse_number}
        """.stripMargin
        .map(_.string("sex"))
        .single
        .apply
        .get

    Util.sex(state)
  }

  val enterTimes: Double = {
    sql"""
         |select
         |  count(*) as count
         |from
         |  race_result
         |inner join
         |  race_info
         |on
         |  race_result.race_id = race_info.id
         |where
         |  horse_id = ${horse_id}
         |and
         |  race_info.date < ${date}
      """.stripMargin
      .map(_.double("count"))
      .single
      .apply
      .get
  }

  val odds: Option[Double] = {
    sql"""
         |select
         |  odds
         |from
         |  race_result
         |where
         |  race_id = ${race_id}
         |and
         |  horse_number = ${horse_number}
      """.stripMargin
      .map(_.doubleOpt("odds"))
      .single
      .apply
      .get
  }

  //Total prize money earnings (finishing first, second or third) to date/Number of races entered
  val jEps: Option[Double] = {
    val earning_money =
      sql"""
           |select
           |  earning_money
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  jockey_id = ${jockey_id}
           |and
           |  race_info.date < ${date}
           |order by date desc
           |limit 100
        """.stripMargin
        .map(_.doubleOpt("earning_money"))
        .list
        .apply()

    if (earning_money.isEmpty)
      None
    else
      Some(earning_money.flatten.sum / earning_money.size)
  }

  val jAvgWin4: Option[Double] = {
    val wins =
      sql"""
           |select
           |  (order_of_finish = '1' or order_of_finish = '2' or order_of_finish = '3') as is_win
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  jockey_id = ${jockey_id}
           |and
           |  race_info.date < ${date}
           |and
           |  speed_figure is not null
           |order by date desc
           |limit 4
        """.stripMargin
        .map(_.double("is_win"))
        .list
        .apply()

    if (wins.isEmpty)
      None
    else
      Some(wins.sum / wins.size)
  }

  val month: String = {
    date.split("-")(1)
  }

  //The winning percentage of the jockey in career to date of race
  val jwinper: Option[Double] = jWinperOf(jockey_id)

  def jWinperOf(jockey_id: String)(implicit s: DBSession): Option[Double] = {
    val wins =
      sql"""
           |select
           |  order_of_finish = '1' as is_win
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  jockey_id = ${jockey_id}
           |and
           |  race_info.date < ${date}
           |order by date desc
           |limit 100
        """.stripMargin
        .map(_.double("is_win"))
        .list
        .apply()

    if (wins.nonEmpty)
      Some(wins.sum / wins.size)
    else
      None
  }

  val ridingStrongJockey: Option[Boolean] = {
    val preJockeyIdOpt =
      sql"""
           |select
           |  jockey_id
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  horse_id = ${horse_id}
           |and
           |  race_info.date < ${date}
           |order by date desc
           |limit 1
        """.stripMargin
        .map(_.string("jockey_id"))
        .single
        .apply()

    for {
      preJockeyId <- preJockeyIdOpt
      preWinper <- jWinperOf(preJockeyId)
      winper <- jwinper
    } yield preJockeyId != jockey_id && preWinper < winper
  }

  val preOOF: Option[String] =
    sql"""
         |select
         |  order_of_finish
         |from
         |  race_result
         |inner join
         |  race_info
         |on
         |  race_result.race_id = race_info.id
         |where
         |  horse_id = ${horse_id}
         |and
         |  race_info.date < ${date}
         |order by date desc
         |limit 1
      """.stripMargin
      .map(_.string("order_of_finish"))
      .single
      .apply()


  val pre2OOF: Option[String] = {
    val orders =
      sql"""
           |select
           |  order_of_finish
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  horse_id = ${horse_id}
           |and
           |  race_info.date < ${date}
           |order by date desc
           |limit 2
        """.stripMargin
        .map(_.string("order_of_finish"))
        .list
        .apply()

    if (orders.size == 2)
      orders.lastOption
    else
      None
  }

  val runningStyle: Double = {
    val orders =
      sql"""
           |select
           |  order_of_finish,
           |  pass
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  horse_id = ${horse_id}
           |and
           |  race_info.date < ${date}
           |order by date desc
           |limit 100
        """.stripMargin
        .map(rs => (rs.string("order_of_finish"), rs.string("pass")))
        .list
        .apply

    val diff =
      orders.filter(_._1.forall(c => '0' < c && c < '9'))
        .filterNot(_._2.isEmpty)
        .map{ case (order_of_finish, pass) =>
          val xs = pass.split("-")
          xs.map(_.toInt).sum.toDouble / xs.size - order_of_finish.toInt
        }

    diff.sum / diff.size
  }

  val preLateStart: Boolean = {
    val preRemark =
      sql"""
           |select
           |  remark
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  horse_id = ${horse_id}
           |and
           |  race_info.date < ${date}
           |order by date desc
           |limit 1
        """.stripMargin
        .map(_.stringOpt("remark"))
        .single
        .apply()
        .flatten

    preRemark.nonEmpty && preRemark.get == "出遅れ"
  }

  val lateStartPer: Double = {
    val lateList =
      sql"""
           |select
           |  (remark = '出遅れ') as is_late
           |from
           |  race_result
           |inner join
           |  race_info
           |on
           |  race_result.race_id = race_info.id
           |where
           |  horse_id = ${horse_id}
           |and
           |  race_info.date < ${date}
           |order by date desc
           |limit 100
        """.stripMargin
        .map(_.intOpt("is_late").getOrElse(0))
        .list
        .apply()

    lateList.sum.toDouble / lateList.size
  }

  val preLastPhase: Option[Double] = {
    sql"""
         |select
         |  last_phase
         |from
         |  race_result
         |inner join
         |  race_info
         |on
         |  race_result.race_id = race_info.id
         |where
         |  horse_id = ${horse_id}
         |and
         |  race_info.date < ${date}
         |order by date desc
         |limit 1
      """.stripMargin
      .map(_.doubleOpt("last_phase"))
      .single
      .apply()
      .flatten
  }


  val course: String = {
    Util.course(rawSurface)
  }

  val placeCode: String = {
    sql"""
         |select
         |  place_detail
         |from
         |  race_info
         |where
         |  id = ${race_id}
      """.stripMargin
      .map(_.string("place_detail"))
      .single
      .apply
      .get
      .replaceAll("\\d+回([^\\d]+)\\d+日目", "$1")
  }

  val headCount: Double = {
    sql"""
         |select
         |  count(*) as head_count
         |from
         |  race_result
         |where
         |  race_id = ${race_id}
      """.stripMargin
      .map(_.double("head_count"))
      .single
      .apply
      .get
  }

  private val preRaceIdOpt =
    sql"""
         |select
         |  race_id
         |from
         |  race_result
         |inner join
         |  race_info
         |on
         |  race_result.race_id = race_info.id
         |where
         |  horse_id = ${horse_id}
         |and
         |  race_info.date < ${date}
         |order by date desc
         |limit 1
      """.stripMargin
      .map(_.int("race_id"))
      .single
      .apply()

  val preHeadCount: Option[Double] = {
    for { pre_race_id <- preRaceIdOpt } yield {
      sql"""
           |select
           |  count(*) as head_count
           |from
           |  race_result
           |where
           |  race_id = ${pre_race_id}
        """.stripMargin
        .map(_.double("head_count"))
        .single
        .apply
        .get
    }
  }

  val surfaceChanged: Option[Boolean] = {
    for { pre_race_id <- preRaceIdOpt } yield {
      val info = RaceInfoDao.getById(pre_race_id)
      surface != Util.surface(info.surface)
    }
  }

  val gradeChanged: Option[Int] = {
    for { pre_race_id <- preRaceIdOpt } yield {
      val info = RaceInfoDao.getById(pre_race_id)
      Util.str2cls(info.race_class) - grade
    }
  }

  val preMargin: Option[String] = {
    sql"""
         |select
         |  length
         |from
         |  race_result
         |inner join
         |  race_info
         |on
         |  race_result.race_id = race_info.id
         |where
         |  horse_id = ${horse_id}
         |and
         |  race_info.date < ${date}
         |order by date desc
         |limit 1
      """.stripMargin
      .map(_.stringOpt("length"))
      .single
      .apply()
      .flatten
      .map(Util.margin)
  }

  val femaleOnly: Boolean = {
    race_class.contains("牝")
  }
}

object FeatureGenerator {

  def iterator()(implicit s: DBSession): Iterator[FeatureGenerator] = {

    val race_infos = {
      sql"""
           |select
           |  race_id, horse_number
           |from
           |  race_result
        """.stripMargin
        .map(rs => (rs.int("race_id"), rs.int("horse_number")))
        .list
        .apply
    }

    var count = 0
    val totalCount = race_infos.size.toDouble

    race_infos
      .toIterator
      .map{ case (race_id, horse_number) =>
        count += 1
        if (count % 1000 == 0)
          println("処理中 ... %7.3f％完了".format(100.0 * count / totalCount))
        new FeatureGenerator(race_id, horse_number)
      }
  }

}
