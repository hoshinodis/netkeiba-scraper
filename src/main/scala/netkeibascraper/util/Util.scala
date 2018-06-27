package netkeibascraper.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.util.Try

object Util {

  //weatherの種類
  //List(晴, 曇, 雨, 小雨, 雪)

  //surfaceの種類
  //List(芝右, ダ右, 障芝, 芝右 外, 障芝 ダート, ダ左, 芝左, 障芝 外, 芝左 外, 芝直線, 障芝 外-内, 障芝 内-外, 芝右 内2周)

  //sexの種類
  //List(牡, 牝, セ)

  def weather(s: String): String = {
    val weatherState = Seq("晴", "曇", "雨", "小雨", "雪")
    if (weatherState.contains(s)) s else "他"
  }

  def surface(s: String): String = {
    val surfaceState = Seq("芝", "ダ")
    if (surfaceState.contains(s)) s else "他"
  }

  def sex(s: String): String = {
    val sexState = Seq("牡", "牝", "セ")
    if (sexState.contains(s)) s else "他"
  }

  def course(s: String): String = {
    val courseState = Seq("直線", "右","左","外")
    if (courseState.contains(s)) s else "他"
  }


  def margin(s: String): String = {
    val marginState = Seq("ハナ", "クビ", "アタマ")
    if (marginState.contains(s)) s else "他"
  }

  def str2ticketType(s: String): Int = {
    val ticketStr =
    Seq(
      "単勝",
      "複勝",
      "枠連",
      "馬連",
      "ワイド",
      "馬単",
      "三連複",
      "三連単"
    ).zipWithIndex.toMap
    ticketStr(s)
  }

  /**
    * Dateをyyyy-MM-DDの形式の文字列に変換
    */
  def date2sqliteStr(date: Date): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    "%04d-%02d-%02d".format(
      cal.get(Calendar.YEAR),
      cal.get(Calendar.MONTH)+1,
      cal.get(Calendar.DAY_OF_MONTH))
  }

  val str2clsMap: Array[(String, Int)] =
    Array(
      "オープン",
      "1600万下",
      "1000万下",
      "500万下",
      "未勝利",
      "新馬").zipWithIndex

  def str2cls(s: String): Int = {
    str2clsMap.foreach{ case (a, b) =>
      if (s.contains(a)) return b }
    sys.error("class not found:"+s)
  }

  val positionState: Array[(String, Int)] =
    Array(
      "(降)",
      "(再)",
      "中",
      "取",
      "失",
      "除"
    ).zipWithIndex

  def position2cls(s: String): (Option[Int], Option[Int]) = {
    positionState.foreach{ case (a, b) =>
      if (s.contains(a))
        return (Try(s.replace(a, "").toInt).toOption, Some(b))
    }
    (Try(s.replaceAll("[^\\d]", "").toInt).toOption, None)
  }

  val surfaceStates =
    Array(
      "ダート : 稍重",
      "ダート : 重",
      "ダート : 良",
      "ダート : 不良",
      "芝 : 良",
      "芝 : 稍重",
      "芝 : 重",
      "芝 : 不良")

  val startTimeFormat = new SimpleDateFormat("hh:mm")
  val finishingTimeFormat = new SimpleDateFormat("m:ss.S")

  val example: Array[Any] =
    Array[Any](
      //race_name
      "3歳上500万下",
      //surface
      "芝右",
      //distance
      2600,
      //weather
      "晴",
      //surface
      "芝 : 良",
      //race start
      startTimeFormat.parse("16:15"),
      //race number
      12,
      //surface score
      -4,
      //date
      "2014年9月7日",
      //place_detail
      "2回小倉12日目",
      //class
      "サラ系3歳以上500万下○混○特指(定量)",
      //finish position
      1,
      //frame number
      7,
      //horse number
      14,
      //horse id
      "2011100417",
      //sex
      "牡",
      //age
      3,
      //basis weight
      54,
      //jockey id
      "01115",
      //finishing time
      finishingTimeFormat.parse("2:39.7"),
      //length
      "",
      //speed figure
      98,
      //pass
      "15-14-11-7",
      //
      35.5,
      //odds
      3.5,
      //integer popularity not null
      1,
      //text horse weight
      "466(+4)",
      //text
      "",
      //text
      "",
      //text remark
      "出遅れ",
      //text stable not null
      "西",
      //text trainer_id not null
      "01071",
      //text owner_id not null
      "708800",
      //real earning_money
      730.0
    )

}