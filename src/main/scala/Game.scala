package mlb

import zio.json._
import zio.jdbc._

import java.time.LocalDate

/** 
 * Provides types and utilities related to game dates
 */
object GameDates {

    /**
      * Represents a game date
      */
    opaque type GameDate = LocalDate

    object GameDate {
        /**
         * Creates a GameDate instance from a LocalDate value.
         * @param value The LocalDate value to create a GameDate from
         * @return The GameDate instance
         */
        def apply(value: LocalDate): GameDate = value

        /**
         * Extracts the LocalDate value from a GameDate instance.
         * @param gameDate The GameDate instance to extract the value from
         * @return The LocalDate value
         */
        def unapply(gameDate: GameDate): LocalDate = gameDate
    }

    given CanEqual[GameDate, GameDate] = CanEqual.derived
    implicit val gameDateEncoder: JsonEncoder[GameDate] = JsonEncoder.localDate
    implicit val gameDateDecoder: JsonDecoder[GameDate] = JsonDecoder.localDate
}

/** Provides types and utilities related to season years */
object SeasonYears {

    /** Represents a season year */
    opaque type SeasonYear <: Int = Int

    object SeasonYear {
        /**
         * Creates a SeasonYear instance from an Int value.
         * @param year The Int value representing the season year
         * @return The SeasonYear instance
         */
        def apply(year: Int): SeasonYear = year

        /**
         * Creates a safe SeasonYear instance from an Int value, ensuring it is within the valid range.
         * @param value The Int value representing the season year
         * @return An Option containing the SeasonYear instance if the value is within the valid range, None otherwise
         */
        def safe(value: Int): Option[SeasonYear] =
            Option.when(value >= 1876 && value <= LocalDate.now.getYear)(value)

        /**
         * Extracts the Int value from a SeasonYear instance.
         * @param seasonYear The SeasonYear instance to extract the value from
         * @return The Int value representing the season year
         */
        def unapply(seasonYear: SeasonYear): Int = seasonYear
    }

    given CanEqual[SeasonYear, SeasonYear] = CanEqual.derived
    implicit val seasonYearEncoder: JsonEncoder[SeasonYear] = JsonEncoder.int
    implicit val seasonYearDecoder: JsonDecoder[SeasonYear] = JsonDecoder.int
}

/** Provides types and utilities related to playoff rounds */
object PlayoffRounds {

    /** Represents a playoff round */
    opaque type PlayoffRound <: Int = Int

    object PlayoffRound {
        /**
         * Creates a PlayoffRound instance from an Int value.
         * @param round The Int value representing the playoff round
         * @return The PlayoffRound instance
         */
        def apply(round: Int): PlayoffRound = round

        /**
         * Creates a safe PlayoffRound instance from an Int value, ensuring it is within the valid range.
         * @param value The Int value representing the playoff round
         * @return An Option containing the PlayoffRound instance if the value is within the valid range, None otherwise
         */
        def safe(value: Int): Option[PlayoffRound] =
            Option.when(value >= 1 && value <= 4)(value)

        /**
         * Extracts the Int value from a PlayoffRound instance.
         * @param playoffRound The PlayoffRound instance to extract the value from
         * @return The Int value representing the playoff round
         */
        def unapply(playoffRound: PlayoffRound): Int = playoffRound
    }

    given CanEqual[PlayoffRound, PlayoffRound] = CanEqual.derived
    implicit val playoffRoundEncoder: JsonEncoder[PlayoffRound] = JsonEncoder.int
    implicit val playoffRoundDEncoder: JsonDecoder[PlayoffRound] = JsonDecoder.int
}


/** trait for representing teams */
trait Teams {
    type T = String
    given CanEqual[T, T] = CanEqual.derived
    implicit val encoder: JsonEncoder[T] = JsonEncoder.string
    implicit val decoder: JsonDecoder[T] = JsonDecoder.string
}

/** Provides types and utilities related to home teams */
object HomeTeams extends Teams {

    /** Represents a home team */
    opaque type HomeTeam = String
    override type T = HomeTeam

    object HomeTeam {
        /**
         * Creates a HomeTeam instance from a String value.
         * @param value The String value representing the home team
         * @return The HomeTeam instance
         */
        def apply(value: String): HomeTeam = value

        /**
         * Extracts the String value from a HomeTeam instance.
         * @param homeTeam The HomeTeam instance to extract the value from
         * @return The String value representing the home team
         */
        def unapply(homeTeam: HomeTeam): String = homeTeam
    }
}

/** Provides types and utilities related to away teams */
object AwayTeams extends Teams {

    /** Represents an away team */
    opaque type AwayTeam = String
    override type T = AwayTeam

    object AwayTeam {
        /**
         * Creates an AwayTeam instance from a String value.
         * @param value The String value representing the away team
         * @return The AwayTeam instance
         */
        def apply(value: String): AwayTeam = value

        /**
         * Extracts the String value from anAwayTeam instance.
         * @param awayTeam The AwayTeam instance to extract the value from
         * @return The String value representing the away team
         */
        def unapply(awayTeam: AwayTeam): String = awayTeam
    }
}

/** Trait for representing players */
trait Players {
    type T = String
    given CanEqual[T, T] = CanEqual.derived
    implicit val encoder: JsonEncoder[T] = JsonEncoder.string
    implicit val decoder: JsonDecoder[T] = JsonDecoder.string
}

/** Provides types and utilities related to home players */
object HomePlayers extends Players {
    /** Represents a home player */
    opaque type HomePlayer = String
    override type T = HomePlayer

    object HomePlayer {
        /**
         * Creates a HomePlayer instance from a String value.
         * @param value The String value representing the home player
         * @return The HomePlayer instance
         */
        def apply(value: String): HomePlayer = value

        /**
         * Extracts the String value from a HomePlayer instance.
         * @param homePlayer The HomePlayer instance to extract the value from
         * @return The String value representing the home player
         */
        def unapply(homePlayer: HomePlayer): String = homePlayer
    }
}

/** Provides types and utilities related to away players */
object AwayPlayers extends Players {
    /** Represents an away player */
    opaque type AwayPlayer = String
    override type T = AwayPlayer

    object AwayPlayer {
        /**
         * Creates an AwayPlayer instance from a String value.
         * @param value The String value representing the away player
         * @return The AwayPlayer instance
         */
        def apply(value: String): AwayPlayer = value

        /**
         * Extracts the String value from an AwayPlayer instance.
         * @param awayPlayer The AwayPlayer instance to extract the value from
         * @return The String value representing the away player
         */
        def unapply(awayPlayer: AwayPlayer): String = awayPlayer
    }
}

/** Trait for representing scores */
trait Scores {
    type T = Int
    given CanEqual[T, T] = CanEqual.derived
    implicit val encoder: JsonEncoder[T] = JsonEncoder.int
    implicit val decoder: JsonDecoder[T] = JsonDecoder.int
}

/** Provides types and utilities related to home scores */
object HomeScores extends Scores{

    /** Represents a home score */
    opaque type HomeScore <: Int = Int
    override type T = HomeScore

    object HomeScore {
        /**
         * Creates a HomeScore instance from an Int value.
         * @param score The Int value representing the home score
         * @return The HomeScore instance
         */
        def apply(score: Int): HomeScore = score
        /**
         * Creates a safe HomeScore instance from an Int value, ensuring it is non-negative.
         * @param value The Int value representing the home score
         * @return An Option containing the HomeScore instance if the value is non-negative, None otherwise
         */
        def safe(value: Int): Option[HomeScore] =
            Option.when(value >= 0)(value)
        /**
         * Extracts the Int value from a HomeScore instance.
         * @param score The HomeScore instance to extract the value from
         * @return The Int value representing the home score
         */
        def unapply(score: HomeScore): Int = score
    }
}

/** Provides types and utilities related to away scores */
object AwayScores extends Scores{
    /** Represents an away score */
    opaque type AwayScore <: Int = Int
    override type T = AwayScore
    object AwayScore {
        /**
         * Creates an AwayScore instance from an Int value.
         * @param score The Int value representing the away score
         * @return The AwayScore instance
         */
        def apply(score: Int): AwayScore = score
        /**
         * Creates a safe AwayScore instance from an Int value, ensuring it is non-negative.
         * @param value The Int value representing the away score
         * @return An Option containing the AwayScore instance if the value is non-negative, None otherwise
         */
        def safe(value: Int): Option[AwayScore] =
            Option.when(value >= 0)(value)
        /**
         * Extracts the Int value from an AwayScore instance.
         * @param score The AwayScore instance to extract the value from
         * @return The Int value representing the away score
         */
        def unapply(score: AwayScore): Int = score
    }
}

/** Trait for representing Elo ratings */
trait Elos {
    type T = Double
    given CanEqual[T, T] = CanEqual.derived
    implicit val encoder: JsonEncoder[T] = JsonEncoder.double
    implicit val decoder: JsonDecoder[T] = JsonDecoder.double
}

/** Provides types and utilities related to home Elo ratings */
object HomeElos extends Elos{
    /** Represents a home Elo rating */
    opaque type HomeElo <: Double = Double
    override type T = HomeElo
    object HomeElo {
        /**
         * Creates a HomeElo instance from a Double value.
         * @param rating The Double value representing the home Elo rating
         * @return The HomeElo instance
         */
        def apply(rating: Double): HomeElo = rating
        /**
         * Creates a safe HomeElo instance from a Double value, ensuring it is non-negative.
         * @param value The Double value representing the home Elo rating
         * @return An Option containing the HomeElo instance if the value is non-negative, None otherwise
         */
        def safe(value: Double): Option[HomeElo] =
            Option.when(value >= 0)(value)
        /**
         * Extracts the Double value from a HomeElo instance.
         * @param rating The HomeElo instance to extract the value from
         * @return The Double value representing the home Elo rating
         */
        def unapply(rating: HomeElo): Double = rating
    }
}

/** Provides types and utilities related to away Elo ratings */
object AwayElos extends Elos{
    /** Represents an away Elo rating */
    opaque type AwayElo <: Double = Double
    override type T = AwayElo
    object AwayElo {
        /**
         * Creates an AwayElo instance from a Double value.
         * @param rating The Double value representing the away Elo rating
         * @return The AwayElo instance
         */
        def apply(rating: Double): AwayElo = rating
        /**
         * Creates a safe AwayElo instance from a Double value, ensuring it is non-negative.
         * @param value The Double value representing the away Elo rating
         * @return An Option containing the AwayElo instance if the value is non-negative, None otherwise
         */
        def safe(value: Double): Option[AwayElo] =
            Option.when(value >= 0)(value)
        /**
         * Extracts the Double value from an AwayElo instance.
         * @param rating The AwayElo instance to extract the value from
         * @return The Double value representing the away Elo rating
         */
        def unapply(rating: AwayElo): Double = rating
    }
}

/** Common trait for representing MLB ratings */
trait Mlbs {
    type T = Double
    given CanEqual[T, T] = CanEqual.derived
    implicit val encoder: JsonEncoder[T] = JsonEncoder.double
    implicit val decoder: JsonDecoder[T] = JsonDecoder.double
}

/** Provides types and utilities related to home MLB ratings */
object HomeMlbs extends Mlbs{
    /** Represents a home MLB rating */
    opaque type HomeMlb <: Double = Double
    override type T = HomeMlb
    object HomeMlb {
        /**
         * Creates a HomeMlb instance from a Double value.
         * @param rating The Double value representing the home MLB rating
         * @return The HomeMlb instance
         */
        def apply(rating: Double): HomeMlb = rating
        /**
         * Creates a safe HomeMlb instance from a Double value, ensuring it is non-negative.
         * @param value The Double value representing the home MLB rating
         * @return An Option containing the HomeMlb instance if the value is non-negative, None otherwise
         */
        def safe(value: Double): Option[HomeMlb] =
            Option.when(value >= 0)(value)
        /**
         * Extracts the Double value from a HomeMlb instance.
         * @param rating The HomeMlb instance to extract the value from
         * @return The Double value representing the home MLB rating
         */
        def unapply(rating: HomeMlb): Double = rating
    }
}

/** Provides types and utilities related to away MLB ratings */
object AwayMlbs extends Mlbs{
    /** Represents an away MLB rating */
    opaque type AwayMlb <: Double = Double
    override type T = AwayMlb
    object AwayMlb {
        /**
         * Creates an AwayMlb instance from a Double value.
         * @param rating The Double value representing the away MLB rating
         * @return The AwayMlb instance
         */
        def apply(rating: Double): AwayMlb = rating
        /**
         * Creates a safe AwayMlb instance from a Double value, ensuring it is non-negative.
         * @param value The Double value representing the away MLB rating
         * @return An Option containing the AwayMlb instance if the value is non-negative, None otherwise
         */
        def safe(value: Double): Option[AwayMlb] =
            Option.when(value >= 0)(value)
        /**
         * Extracts the Double value from an AwayMlb instance.
         * @param rating The AwayMlb instance to extract the value from
         * @return The Double value representing the away MLB rating
         */
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

/**
 * Represents a game.
 * @param date The date of the game
 * @param season The season year of the game
 * @param playoff The playoff round of the game (optional)
 * @param homeTeam The home team of the game
 * @param awayTeam The away team of the game 
 * @param homePlayer The home player of the game
 * @param awayPlayer The away player of the game
 * @param homeScore The score of the home team
 * @param awayScore The score of the away team
 * @param homeElo The Elo rating of the home team
 * @param awayElo The Elo rating of the away team
 * @param homeMlb The MLB rating of the home team
 * @param awayMlb The MLB rating of the away team
 */
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

  /**
   * Extracts the values from a Game instance.
   * @param game The Game instance to extract the values from
   * @return A tuple containing the extracted values in order: (date, season, playoff, homeTeam, awayTeam, homePlayer, awayPlayer, homeScore, awayScore, homeElo, awayElo, homeMlb, awayMlb)
   */
  def unapply(game: Game): (GameDate, SeasonYear, Option[PlayoffRound], HomeTeam, AwayTeam, HomePlayer, AwayPlayer, HomeScore, AwayScore, HomeElo, AwayElo, HomeMlb, AwayMlb) =
    (game.date, game.season, game.playoff, game.homeTeam, game.awayTeam, game.homePlayer, game.awayPlayer, game.homeScore, game.awayScore, game.homeElo, game.awayElo, game.homeMlb, game.awayMlb)

  // a custom decoder from a tuple
  type Row = (String, Int, Option[Int], String, String, String, String, Int, Int, Double, Double, Double, Double)

  /**
   * Converts a Game instance to a tuple representation.
   * @param g The Game instance to convert
   * @return A tuple representation of the Game instance
   */
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

  /**
   * Custom JDBC decoder for decoding a Game instance from a database row.
   */
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
