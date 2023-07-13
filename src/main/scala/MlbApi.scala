package mlb

import zio._
import zio.jdbc._
import zio.http._
import zio.stream._

import java.time.LocalDate
import java.sql.Date
import com.github.tototoshi.csv._
import scala.util.Try

/**
 * Provides the MLB API functionality.
 */
object MlbApi extends ZIOAppDefault {

  import DataService._
  import ApiService._
  import HomeTeams._
  import AwayTeams._

  /**
   * Defines the static routes for the API.
   */
  val static: App[Any] = Http.collect[Request] {
    case Method.GET -> Root / "text" => Response.text("Hello MLB Fans!")
    case Method.GET -> Root / "json" => Response.json("""{"greetings": "Hello MLB Fans!"}""")
  }.withDefaultErrorResponse

  /**
   * Defines the API endpoints.
   */
  val endpoints: App[ZConnectionPool] = Http.collectZIO[Request] {
    case Method.GET -> Root / "init" =>
      ZIO.succeed(Response.text("Not Implemented").withStatus(Status.NotImplemented))
    case Method.GET -> Root / "game" / "latest" / homeTeam / awayTeam =>
      // Retrieve the latest game between the specified home team and away team
      for {
        game: Option[Game] <- latest(HomeTeam(homeTeam), AwayTeam(awayTeam))
        res: Response = latestGameResponse(game)
      } yield res
    case Method.GET -> Root / "game" / "predict" / homeTeam / awayTeam =>
      // Predict the outcome of the game between the specified home team and away team
      for {
        gameA: Option[Game] <- realLatest(homeTeam)
        gameB: Option[Game] <- realLatest(awayTeam)
        res: Response = predictionResponse(homeTeam, awayTeam, gameA, gameB)
      } yield res
    case Method.GET -> Root / "games" / "count" =>
      // Count the number of games in the data
      for {
        count: Option[Int] <- count
        res: Response = countResponse(count)
      } yield res
    case Method.GET -> Root / "games" / "history" / homeTeam =>
      // Retrieve the game history for the specified team
      import zio.json.EncoderOps
      import Game._
      for {
        games: Chunk[Game] <- historyTeam(homeTeam)
        res: Response = historyResponse(games)
      } yield res
    case Method.GET -> Root / "pitcher" / "history" / pitcher =>
      // Retrieve the game history for the specified pitcher
      import zio.json.EncoderOps
      import Game._
      for {
        games: Chunk[Game] <- historyPitcher(pitcher)
        res: Response = historyResponse(games)
      } yield res
    case _ =>
      // Endpoint not found
      ZIO.succeed(Response.text("Not Found").withStatus(Status.NotFound))
  }.withDefaultErrorResponse

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
   * Converts a CSV row to a Game object.
   *
   * @param csvRow The CSV row
   * @return An option of Game object
   */
  def convertCsvSeqToGame(csvRow: Seq[String]): Option[Game] = for {
    date <- Try(LocalDate.parse(csvRow(0))).toOption.map(GameDate.apply)
    homeTeam = HomeTeam(csvRow(4))
    awayTeam = AwayTeam(csvRow(5))
    homePlayer = HomePlayer(csvRow(14))
    awayPlayer = AwayPlayer(csvRow(15))
    playoffRound: Option[PlayoffRound] = csvRow(3).toIntOption.flatMap(PlayoffRound.safe)
    sy <- csvRow(1).toIntOption.flatMap(SeasonYear.safe)
    hs <- csvRow(24).toIntOption.flatMap(HomeScore.safe)
    as <- csvRow(25).toIntOption.flatMap(AwayScore.safe)
    he <- csvRow(10).toDoubleOption.flatMap(HomeElo.safe)
    ae <- csvRow(11).toDoubleOption.flatMap(AwayElo.safe)
    hm <- csvRow(22).toDoubleOption.flatMap(HomeMlb.safe)
    am <- csvRow(23).toDoubleOption.flatMap(AwayMlb.safe)
  } yield Game(date, sy, playoffRound, homeTeam, awayTeam, homePlayer, awayPlayer, hs, as, he, ae, hm, am)

  /**
   * The main application logic.
   */
  val app: ZIO[ZConnectionPool & Server, Throwable, Unit] = for {
    _ <- Console.printLine("Creation of the table")
    conn <- create
    _ <- Console.printLine("Opening the csv file")
    source <- ZIO.succeed(CSVReader.open("mlb_elo_latest.csv"))
    _ <- Console.printLine("Convert csv to the game structure & insertion of rows into the DB")
    stream = ZStream
      .fromIterator[Seq[String]](source.iterator)
      .map[Option[Game]](convertCsvSeqToGame)
      .collectSome //retrieve all without None value
      .runCollect
    streamChunk <- stream
    chunkToList = streamChunk.toList
    _ <- insertRows(chunkToList)
    _ <- Console.print("Insertion of rows succeed")
    _ <- ZIO.succeed(source.close())
    test <- latest(HomeTeam("CHW"), AwayTeam("DET"))
    _ <- Console.printLine(test)
    _ <- Server.serve[ZConnectionPool](static ++ endpoints)
  } yield ()

  override def run: ZIO[Any, Throwable, Unit] =
    app.provide(createZIOPoolConfig >>> connectionPool, Server.default)
}

object ApiService {

  import zio.json.EncoderOps
  import Game._
  import HomeTeams._
  import AwayTeams._

  /**
   * Constructs a response for the count endpoint.
   *
   * @param count The count of games
   * @return The response
   */
  def countResponse(count: Option[Int]): Response = {
    count match
      case Some(c) => Response.text(s"$c game(s) in historical data").withStatus(Status.Ok)
      case None => Response.text("No game in historical data").withStatus(Status.NotFound)
  }

  /**
   * Constructs a response for the latest game endpoint.
   *
   * @param game The latest game
   * @return The response
   */
  def latestGameResponse(game: Option[Game]): Response = {
    println(game)
    game match
      case Some(g) => Response.json(g.toJson).withStatus(Status.Ok)
      case None => Response.text("No game found in historical data").withStatus(Status.NotFound)
  }

  /**
   * Constructs a response for the game history endpoint.
   *
   * @param games The game history
   * @return The response
   */
  def historyResponse(games: zio.Chunk[Game]): Response = {
    if(games.isEmpty) then
      Response.text("No game was found")
    else
      Response.json(games.toJson).withStatus(Status.Ok)
  }

  /**
   * Constructs a response for the game prediction endpoint.
   *
   * @param homeTeam The home team name
   * @param awayTeam The away team name
   * @param gameA The latest game for the home team
   * @param gameB The latest game for the away team
   * @return The response
   */
  def predictionResponse(homeTeam: String, awayTeam: String, gameA: Option[Game], gameB: Option[Game]): Response = {
    // Retrieve the Elo ratings for the home team
    val maybeEloA = gameA.flatMap { game =>
    if game.homeTeam == HomeTeam(homeTeam) then Some(game.homeElo)
    else if game.awayTeam == AwayTeam(homeTeam) then Some(game.awayElo)
    else None
  }
  
    // Retrieve the Elo ratings for the away team
    val maybeEloB = gameB.flatMap { game =>
      if game.homeTeam == HomeTeam(awayTeam) then Some(game.homeElo)
      else if game.awayTeam == AwayTeam(awayTeam) then Some(game.awayElo)
      else None
    }

    // Calculate the win probability based on Elo ratings
    val result = (maybeEloA, maybeEloB) match {
      case (Some(eloA), Some(eloB)) =>
        val prediction: Double = 1 / (1 + math.pow(10, (eloB - eloA) / 400))
        Response.text(s"$homeTeam vs $awayTeam win probability: $prediction")
      case _ =>
        Response.text("An error happened")
    }

    result
  }
}