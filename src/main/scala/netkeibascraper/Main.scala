package netkeibascraper

import netkeibascraper.feature.FeatureDao
import netkeibascraper.payoff.PayoffDao
import netkeibascraper.raceinfo.RaceInfoDao
import netkeibascraper.raceresult.RaceResultDao
import netkeibascraper.scraper.{RaceListScraper, RaceScraper}
import netkeibascraper.util.RowExtractor
import scalikejdbc._

object Main {

  def initDBSetting(): Unit = {
    //Class.forName("org.sqlite.JDBC")
    //ConnectionPool.singleton("jdbc:sqlite:race.db", null, null)
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton("jdbc:mysql://localhost/netkeiba", "root", null)
  }

  def main(args: Array[String]): Unit = {
    args.headOption match {
      case Some("collecturl") =>
        //レース結果が載っているURLを収集して「race_url.txt」に保存する。
        val pastYears = 1 //過去???年分のURLを収集する
        RaceListScraper.scrape(period = 12 * pastYears)
      case Some("scrapehtml") =>
        //レース結果のHTMLをスクレイピングしてhtmlフォルダに保存する。HTMLをまるごとスクレイピングするので結構時間がかかる。
        RaceScraper.scrape()
      case Some("extract") =>
        //HTMLからレース結果を抜き出しDBに保存する。
        initDBSetting()
        DB.localTx { implicit s =>
          RaceInfoDao.createTable()
          RaceResultDao.createTable()
          PayoffDao.createTable()
          RowExtractor.extract()
        }
      case Some("genfeature") =>
        //レース結果を元にして素性を作りDBに保存する。
        GlobalSettings.loggingSQLAndTime = new LoggingSQLAndTimeSettings(
          enabled = false,
          logLevel = 'info
        )
        initDBSetting()
        DB.localTx { implicit s =>
          FeatureDao.createTable()
        }
        FeatureDao.rr2f()
      case _ =>
        sys.error("invalid argument")
    }
  }
}
