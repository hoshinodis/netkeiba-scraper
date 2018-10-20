package netkeibascraper.scraper

import java.io.File

import org.apache.commons.io.FileUtils
import org.openqa.selenium.By
import org.openqa.selenium.htmlunit.HtmlUnitDriver

object RaceScraper {

  val mail = "enter your email address"
  val password = "enter your password"

  def scrape(): Unit = {

    val driver = new HtmlUnitDriver(false)

    //login
    driver.get("https://account.netkeiba.com/?pid=login")
    driver.findElement(By.name("login_id")).sendKeys(mail)
    driver.findElement(By.name("pswd")).sendKeys(password+"\n")

    val re = """/race/(\d+)/""".r

    val nums =
      io.Source.fromFile("race_url.txt").getLines.toList
        .map{ s => val re(x) = re.findFirstIn(s).get; x }

    val urls = nums.map(s => "http://db.netkeiba.com/race/" + s)

    val folder = new File("html")
    if (!folder.exists()) folder.mkdir()

    var i = 0

    nums.zip(urls).foreach {
      case (num, url) =>
        i += 1
        println(""+i+":downloading "+url)
        val file = new File(folder, num + ".html")
        if (!file.exists()) {
          driver.get(url)
          //↓ここあんまり短くしないでね！
          Thread.sleep(5000)
          val html = driver.getPageSource
          FileUtils.writeStringToFile(file, html)
      }
    }
  }

}
