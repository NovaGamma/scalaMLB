package mlb

import zio._
import zio.jdbc._
import zio.http._


object DataService {

  val createZIOPoolConfig: ULayer[ZConnectionPoolConfig] =
    ZLayer.succeed(ZConnectionPoolConfig.default)

  val properties: Map[String, String] = Map(
    "user" -> "postgres",
    "password" -> "postgres"
  )

  val connectionPool: ZLayer[ZConnectionPoolConfig, Throwable, ZConnectionPool] =
    ZConnectionPool.h2mem(
      database = "mlb",
      props = properties
    )

  val create: ZIO[ZConnectionPool, Throwable, Unit] = transaction {
      execute(sql"""
          CREATE TABLE IF NOT EXISTS Games (
          date VARCHAR(255),
          season_year INTEGER,
          playoff_round VARCHAR(255),
          home_team VARCHAR(3),
          away_team VARCHAR(3),
          home_player VARCHAR(255),
          away_player VARCHAR(255),
          home_score INTEGER,
          away_score INTEGER
          );"""
      );
    }

  import GameDates.*
  import PlayoffRounds.*
  import SeasonYears.*
  import HomeTeams.*
  import AwayTeams.*

  // Should be implemented to replace the `val insertRows` example above. Replace `Any` by the proper case class.
  def insertRows(games: List[Game]): ZIO[ZConnectionPool, Throwable, UpdateResult] = {
    val rows: List[Game.Row] = games.map(_.toRow)
        transaction {
            insert(
                sql"INSERT INTO games(date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score)".values[Game.Row](rows)
            )
        }
  }

  val count: ZIO[ZConnectionPool, Throwable, Option[Int]] = transaction {
    selectOne(
      sql"SELECT COUNT(*) FROM games".as[Int]
    )
  }

  def latest(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, Option[Game]] = {
    transaction {
      selectOne(
        sql"SELECT date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score FROM games WHERE home_team = ${HomeTeam.unapply(homeTeam)} AND away_team = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 1".as[Game]
      )
    }
  }
}
