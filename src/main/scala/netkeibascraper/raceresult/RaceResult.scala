package netkeibascraper.raceresult

case class RaceResult (
  race_id: Int,
  order_of_finish: String,
  frame_number: Int,
  horse_number: Int,
  horse_id: String,
  sex: String,
  age: Int,
  basis_weight: Double,
  jockey_id: String,
  finishing_time: String,
  length: String,
  speed_figure: Option[Int],
  pass: String,
  last_phase: Option[Double],
  odds: Option[Double],
  popularity: Option[Int],
  horse_weight: String,
  remark: Option[String],
  stable: String,
  trainer_id: String,
  owner_id: String,
  earning_money: Option[Double]
)
