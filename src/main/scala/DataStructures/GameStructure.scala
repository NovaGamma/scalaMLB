package mlb

case class Game(
    id: Int,
    date: String,
    season: Int,
    neutral: Boolean,
    playoff: String,
    team1_id: Team,
    team2_id: Team,
    pitcher1_id: Player,
    pitcher2_id: Player,
    score1: Int,
    score2: Int
)