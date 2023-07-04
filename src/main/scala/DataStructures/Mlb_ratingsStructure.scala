package mlb

case class Mlb_ratings(
    id: Int,
    team_id: Team,
    game_id: Game,
    rating_pre: Float,
    rating_prob: Float,
    rating_post: Float,

)