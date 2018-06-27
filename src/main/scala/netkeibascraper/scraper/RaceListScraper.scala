package netkeibascraper.scraper

import java.io.File

import org.apache.commons.io.FileUtils

object RaceListScraper {

  def extractRaceList(baseUrl: String): List[String] = {
    "/race/list/\\d+/".r.findAllIn(io.Source.fromURL(baseUrl, "EUC-JP").mkString).toList
      .map("http://db.netkeiba.com" + _)
      .distinct
  }

  def extractPrevMonth(baseList: String): String = {
    "/\\?pid=[^\"]+".r.findFirstIn(io.Source.fromURL(baseList, "EUC-JP")
      .getLines
      .filter(_.contains("race_calendar_rev_02.gif")).toList.head)
      .map("http://db.netkeiba.com" + _).get
  }

  def extractRace(listUrl: String): List[String] = {
    "/race/\\d+/".r.findAllIn(io.Source.fromURL(listUrl, "EUC-JP").mkString).toList
      .map("http://db.netkeiba.com" + _)
      .distinct
  }

  def scrape(period: Int): Unit = {
    var baseUrl = "http://db.netkeiba.com/?pid=race_top"
    var i = 0

    while (i < period) {
      Thread.sleep(1000)
      val raceListPages =
        extractRaceList(baseUrl)
      val racePages =
        raceListPages.flatMap { url =>
          Thread.sleep(1000)
          extractRace(url)
        }

      racePages.foreach{ url =>
        FileUtils.writeStringToFile(new File("race_url.txt"), url + "\n", true)
      }

      baseUrl = extractPrevMonth(baseUrl)
      println(i + ": collecting URLs from " + baseUrl)
      i += 1
    }
  }

}
