package mlb

case class Pitcher_ratingsStructure_ratings(
    id: Int,
    player_id: Player,
    game_id: Game,
    rgs: Float,
    adj: Float
)
