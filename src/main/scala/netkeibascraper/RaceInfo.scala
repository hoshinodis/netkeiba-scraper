package netkeibascraper

case class RaceInfo (
  race_name: String,
  surface: String,
  distance: Int,
  weather: String,
  surface_state: String,
  race_start: String,
  race_number: Int,
  surface_score: Option[Int],
  date: String,
  place_detail: String,
  race_class: String
)
