package mlb

import zio._
import zio.jdbc._
import zio.http._

import java.sql.Date

object MlbApi extends ZIOAppDefault {

  import DataService._
  import ApiService._
  import HomeTeams._
  import AwayTeams._

  val static: App[Any] = Http.collect[Request] {
    case Method.GET -> Root / "text" => Response.text("Hello MLB Fans!")
    case Method.GET -> Root / "json" => Response.json("""{"greetings": "Hello MLB Fans!"}""")
  }.withDefaultErrorResponse

  val endpoints: App[ZConnectionPool] = Http.collectZIO[Request] {
    case Method.GET -> Root / "init" =>
      ZIO.succeed(Response.text("Not Implemented").withStatus(Status.NotImplemented))
    case Method.GET -> Root / "game" / "latest" / homeTeam / awayTeam =>
      for {
        game: Option[Game] <- latest(HomeTeam(homeTeam), AwayTeam(awayTeam))
        res: Response = latestGameResponse(game)
      } yield res
    case Method.GET -> Root / "game" / "predict" / homeTeam / awayTeam =>
      for {
        gameA: Option[Game] <- realLatest(homeTeam)
        gameB: Option[Game] <- realLatest(awayTeam)
        res: Response = predictionResponse(homeTeam, awayTeam, gameA, gameB)
      } yield res
    case Method.GET -> Root / "games" / "count" =>
      for {
        count: Option[Int] <- count
        res: Response = countResponse(count)
      } yield res
    case Method.GET -> Root / "games" / "history" / homeTeam =>
      import zio.json.EncoderOps
      import Game._
      for {
        games: Chunk[Game] <- historyTeam(homeTeam)
      } yield ZIO.succeed(Response.json(games.toJson).withStatus(Status.Ok))
    case _ =>
      ZIO.succeed(Response.text("Not Found").withStatus(Status.NotFound))
  }.withDefaultErrorResponse

  val appLogic: ZIO[ZConnectionPool & Server, Throwable, Unit] = for {
    _ <- create *> insertRows
    _ <- Server.serve[ZConnectionPool](static ++ endpoints)
  } yield ()

  override def run: ZIO[Any, Throwable, Unit] =
    appLogic.provide(createZIOPoolConfig >>> connectionPool, Server.default)
}

object ApiService {

  import zio.json.EncoderOps
  import Game._
  import HomeTeams._
  import AwayTeams._

  def countResponse(count: Option[Int]): Response = {
    count match
      case Some(c) => Response.text(s"$c game(s) in historical data").withStatus(Status.Ok)
      case None => Response.text("No game in historical data").withStatus(Status.NotFound)
  }

  def latestGameResponse(game: Option[Game]): Response = {
    println(game)
    game match
      case Some(g) => Response.json(g.toJson).withStatus(Status.Ok)
      case None => Response.text("No game found in historical data").withStatus(Status.NotFound)
  }

  def predictionResponse(homeTeam: String, awayTeam: String, gameA: Option[Game], gameB: Option[Game]): Response = {
    val maybeEloA = gameA.flatMap { game =>
    if game.homeTeam == HomeTeam(homeTeam) then Some(game.homeElo)
    else if game.awayTeam == AwayTeam(homeTeam) then Some(game.awayElo)
    else None
  }
  
    val maybeEloB = gameB.flatMap { game =>
      if game.homeTeam == HomeTeam(awayTeam) then Some(game.homeElo)
      else if game.awayTeam == AwayTeam(awayTeam) then Some(game.awayElo)
      else None
    }

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