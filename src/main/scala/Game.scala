package mlb

import zio.json._
import zio.jdbc._

import java.time.LocalDate

object GameDates {

    opaque type GameDate = LocalDate

    object GameDate {
        def apply(value: LocalDate): GameDate = value
        def unapply(gameDate: GameDate): LocalDate = gameDate
    }

    given CanEqual[GameDate, GameDate] = CanEqual.derived
    implicit val gameDateEncoder: JsonEncoder[GameDate] = JsonEncoder.localDate
    implicit val gameDateDecoder: JsonDecoder[GameDate] = JsonDecoder.localDate
}

object SeasonYears {

    opaque type SeasonYear <: Int = Int

    object SeasonYear {
        def apply(year: Int): SeasonYear = year
        def safe(value: Int): Option[SeasonYear] =
            Option.when(value >= 1876 && value <= LocalDate.now.getYear)(value)
        def unapply(seasonYear: SeasonYear): Int = seasonYear
    }

    given CanEqual[SeasonYear, SeasonYear] = CanEqual.derived
    implicit val seasonYearEncoder: JsonEncoder[SeasonYear] = JsonEncoder.int
    implicit val seasonYearDecoder: JsonDecoder[SeasonYear] = JsonDecoder.int
}

object PlayoffRounds {

    opaque type PlayoffRound <: Int = Int

    object PlayoffRound {
        def apply(round: Int): PlayoffRound = round
        def safe(value: Int): Option[PlayoffRound] =
            Option.when(value >= 1 && value <= 4)(value)
        def unapply(playoffRound: PlayoffRound): Int = playoffRound
    }

    given CanEqual[PlayoffRound, PlayoffRound] = CanEqual.derived
    implicit val playoffRoundEncoder: JsonEncoder[PlayoffRound] = JsonEncoder.int
    implicit val playoffRoundDEncoder: JsonDecoder[PlayoffRound] = JsonDecoder.int
}

trait Teams {
    type T = String
    given CanEqual[T, T] = CanEqual.derived
    implicit val encoder: JsonEncoder[T] = JsonEncoder.string
    implicit val decoder: JsonDecoder[T] = JsonDecoder.string
}

object HomeTeams extends Teams {

    opaque type HomeTeam = String
    override type T = HomeTeam

    object HomeTeam {
        def apply(value: String): HomeTeam = value
        def unapply(homeTeam: HomeTeam): String = homeTeam
    }
}

object AwayTeams extends Teams {

    opaque type AwayTeam = String
    override type T = AwayTeam

    object AwayTeam {
        def apply(value: String): AwayTeam = value
        def unapply(awayTeam: AwayTeam): String = awayTeam
    }
}

trait Players {
    type T = String
    given CanEqual[T, T] = CanEqual.derived
    implicit val encoder: JsonEncoder[T] = JsonEncoder.string
    implicit val decoder: JsonDecoder[T] = JsonDecoder.string
}

object HomePlayers extends Players {
    opaque type HomePlayer = String
    override type T = HomePlayer

    object HomePlayer {
        def apply(value: String): HomePlayer = value
        def unapply(homePlayer: HomePlayer): String = homePlayer
    }
}

object AwayPlayers extends Players {
    opaque type AwayPlayer = String
    override type T = AwayPlayer

    object AwayPlayer {
        def apply(value: String): AwayPlayer = value
        def unapply(awayPlayer: AwayPlayer): String = awayPlayer
    }
}

trait Scores {
    type T = Int
    given CanEqual[T, T] = CanEqual.derived
    implicit val encoder: JsonEncoder[T] = JsonEncoder.int
    implicit val decoder: JsonDecoder[T] = JsonDecoder.int
}

object HomeScores extends Scores{

    opaque type HomeScore <: Int = Int
    override type T = HomeScore

    object HomeScore {
        def apply(score: Int): HomeScore = score
        def safe(value: Int): Option[HomeScore] =
            Option.when(value >= 0)(value)
        def unapply(score: HomeScore): Int = score
    }
}

object AwayScores extends Scores{

    opaque type AwayScore <: Int = Int
    override type T = AwayScore
    object AwayScore {
        def apply(score: Int): AwayScore = score
        def safe(value: Int): Option[AwayScore] =
            Option.when(value >= 0)(value)
        def unapply(score: AwayScore): Int = score
    }
}

trait Elos {
    type T = Double
    given CanEqual[T, T] = CanEqual.derived
    implicit val encoder: JsonEncoder[T] = JsonEncoder.double
    implicit val decoder: JsonDecoder[T] = JsonDecoder.double
}

object HomeElos extends Elos{

    opaque type HomeElo <: Double = Double
    override type T = HomeElo
    object HomeElo {
        def apply(rating: Double): HomeElo = rating
        def safe(value: Double): Option[HomeElo] =
            Option.when(value >= 0)(value)
        def unapply(rating: HomeElo): Double = rating
    }
}

object AwayElos extends Elos{

    opaque type AwayElo <: Double = Double
    override type T = AwayElo
    object AwayElo {
        def apply(rating: Double): AwayElo = rating
        def safe(value: Double): Option[AwayElo] =
            Option.when(value >= 0)(value)
        def unapply(rating: AwayElo): Double = rating
    }
}

trait Mlbs {
    type T = Double
    given CanEqual[T, T] = CanEqual.derived
    implicit val encoder: JsonEncoder[T] = JsonEncoder.double
    implicit val decoder: JsonDecoder[T] = JsonDecoder.double
}

object HomeMlbs extends Mlbs{

    opaque type HomeMlb <: Double = Double
    override type T = HomeMlb
    object HomeMlb {
        def apply(rating: Double): HomeMlb = rating
        def safe(value: Double): Option[HomeMlb] =
            Option.when(value >= 0)(value)
        def unapply(rating: HomeMlb): Double = rating
    }
}

object AwayMlbs extends Mlbs{

    opaque type AwayMlb <: Double = Double
    override type T = AwayMlb
    object AwayMlb {
        def apply(rating: Double): AwayMlb = rating
        def safe(value: Double): Option[AwayMlb] =
            Option.when(value >= 0)(value)
        def unapply(rating: AwayMlb): Double = rating
    }
}

import GameDates.*
import PlayoffRounds.*
import SeasonYears.*
import HomeTeams.*
import AwayTeams.*
import HomeScores.*
import AwayScores.*
import HomePlayers.*
import AwayPlayers.*
import HomeElos.*
import AwayElos.*
import HomeMlbs.*
import AwayMlbs.*

final case class Game(
    date: GameDate,
    season: SeasonYear,
    playoff: Option[PlayoffRound],
    homeTeam: HomeTeam,
    awayTeam: AwayTeam,
    homePlayer: HomePlayer,
    awayPlayer: AwayPlayer,
    homeScore: HomeScore,
    awayScore: AwayScore,
    homeElo: HomeElo,
    awayElo: AwayElo,
    homeMlb: HomeMlb,
    awayMlb: AwayMlb
)

object Game {

  given CanEqual[Game, Game] = CanEqual.derived
  implicit val gameEncoder: JsonEncoder[Game] = DeriveJsonEncoder.gen[Game]
  implicit val gameDecoder: JsonDecoder[Game] = DeriveJsonDecoder.gen[Game]

  def unapply(game: Game): (GameDate, SeasonYear, Option[PlayoffRound], HomeTeam, AwayTeam, HomePlayer, AwayPlayer, HomeScore, AwayScore, HomeElo, AwayElo, HomeMlb, AwayMlb) =
    (game.date, game.season, game.playoff, game.homeTeam, game.awayTeam, game.homePlayer, game.awayPlayer, game.homeScore, game.awayScore, game.homeElo, game.awayElo, game.homeMlb, game.awayMlb)

  // a custom decoder from a tuple
  type Row = (String, Int, Option[Int], String, String, String, String, Int, Int, Double, Double, Double, Double)

  extension (g:Game)
    def toRow: Row =
      val (d, y, p, h, a, hp, ap, sh, sa, eh, ea, mh, ma) = Game.unapply(g)
      (
        GameDate.unapply(d).toString,
        SeasonYear.unapply(y),
        p.map(PlayoffRound.unapply),
        HomeTeam.unapply(h),
        AwayTeam.unapply(a),
        HomePlayer.unapply(hp),
        AwayPlayer.unapply(ap),
        HomeScore.unapply(sh),
        AwayScore.unapply(sa),
        HomeElo.unapply(eh),
        AwayElo.unapply(ea),
        HomeMlb.unapply(mh),
        AwayMlb.unapply(ma)
        
      )

  implicit val jdbcDecoder: JdbcDecoder[Game] = JdbcDecoder[Row]().map[Game] { t =>
      val (date, season, maybePlayoff, home, away, homePlayer, awayPlayer, homeScore, awayScore, homeElo, awayElo, homeMlb, awayMlb) = t
      Game(
        GameDate(LocalDate.parse(date)),
        SeasonYear(season),
        maybePlayoff.map(PlayoffRound(_)),
        HomeTeam(home),
        AwayTeam(away),
        HomePlayer(homePlayer),
        AwayPlayer(awayPlayer),
        HomeScore(homeScore),
        AwayScore(awayScore),
        HomeElo(homeElo),
        AwayElo(awayElo),
        HomeMlb(homeMlb),
        AwayMlb(awayMlb)
      )
    }
}
