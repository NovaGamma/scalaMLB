package mlb

case class Team_player(
    id: Int,
    season: Int,
    team_id: Team,
    player_id: Player,
)
