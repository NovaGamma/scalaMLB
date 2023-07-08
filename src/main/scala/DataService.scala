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
          away_score INTEGER,
          home_elo FLOAT,
          away_elo FLOAT,
          home_mlb FLOAT,
          away_mlb FLOAT
          );"""
      );
    }

  import GameDates.*
  import PlayoffRounds.*
  import SeasonYears.*
  import HomeTeams.*
  import AwayTeams.*
  import HomePlayers.* 
  import AwayPlayers.*

  // Should be implemented to replace the `val insertRows` example above. Replace `Any` by the proper case class.
  def insertRows(games: List[Game]): ZIO[ZConnectionPool, Throwable, UpdateResult] = {
    val rows: List[Game.Row] = games.map(_.toRow)
        transaction {
            insert(
                sql"INSERT INTO games(date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score, home_elo, away_elo, home_mlb, away_mlb)".values[Game.Row](rows)
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
        sql"SELECT date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score, home_elo, away_elo, home_mlb, away_mlb FROM games WHERE home_team = ${HomeTeam.unapply(homeTeam)} AND away_team = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 1".as[Game]
      )
    }
  }

  def historyTeam(team: String): ZIO[ZConnectionPool, Throwable, zio.Chunk[Game]] = {
    transaction {
      selectAll(
        sql"SELECT date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score FROM games WHERE home_team = ${HomeTeam.unapply(HomeTeam(team))} OR away_team = ${AwayTeam.unapply(AwayTeam(team))}".as[Game]
      )
    }
  }

  def historyPitcher(pitcher: String): ZIO[ZConnectionPool, Throwable, zio.Chunk[Game]] = {
    transaction {
      selectAll(
        sql"SELECT date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score FROM games WHERE home_player = ${HomePlayer.unapply(HomePlayer(pitcher))} OR away_player = ${AwayPlayer.unapply(AwayPlayer(pitcher))}".as[Game]
      )
    }
  }

  def realLatest(team: String): ZIO[ZConnectionPool, Throwable, Option[Game]] = {
    transaction {
      selectOne(
        sql"SELECT date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score FROM games WHERE home_team = ${HomeTeam.unapply(HomeTeam(team))} OR away_team = ${AwayTeam.unapply(AwayTeam(team))} ORDER BY date DESC LIMIT 1".as[Game]
      )
    }
  }

  def victoriesAndDefeats(team: String, season: SeasonYear): ZIO[ZConnectionPool, Throwable, Option[zio.jdbc.ZResultSet]] = {
    transaction {
      selectOne(
        sql"SELECT (SELECT COUNT(*) FROM games WHERE season_year = ${SeasonYear.unapply(season)} AND ((home_team = ${HomeTeam.unapply(HomeTeam(team))} AND home_score > away_score) OR(away_team = ${HomeTeam.unapply(HomeTeam(team))} AND away_score > home_score))) AS nb_victory,(SELECT COUNT(*) FROM games     WHERE season_year = ${SeasonYear.unapply(season)} AND ((home_team = ${HomeTeam.unapply(HomeTeam(team))} AND home_score < away_score) OR (away_team = ${HomeTeam.unapply(HomeTeam(team))} AND away_score < home_score))) AS nb_defeat;"
      )
    }
  }
}
