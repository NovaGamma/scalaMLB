package mlb

import zio._
import zio.jdbc._
import zio.http._

/**
 * Provides data base services for the MLB package.
 */
object DataService {

  /**
   * Creates the ZIO layer configuration for the connection pool.
   */
  val createZIOPoolConfig: ULayer[ZConnectionPoolConfig] =
    ZLayer.succeed(ZConnectionPoolConfig.default)

  /**
   * Database connection properties.
   */
  val properties: Map[String, String] = Map(
    "user" -> "postgres",
    "password" -> "postgres"
  )

  /**
   * Creates the connection pool layer.
   */
  val connectionPool: ZLayer[ZConnectionPoolConfig, Throwable, ZConnectionPool] =
    ZConnectionPool.h2mem(
      database = "mlb",
      props = properties
    )

  /**
   * Creates the "Games" table if it doesn't exist.
   */
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

  /**
   * Inserts rows into the "Games" table.
   *
   * @param games The list of games to insert
   * @return The result of the update operation
   */
  def insertRows(games: List[Game]): ZIO[ZConnectionPool, Throwable, UpdateResult] = {
    val rows: List[Game.Row] = games.map(_.toRow)
        transaction {
            insert(
                sql"INSERT INTO games(date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score, home_elo, away_elo, home_mlb, away_mlb)".values[Game.Row](rows)
            )
        }
  }

  /**
   * Counts the number of rows in the "Games" table.
   *
   * @return The count of rows as an option
   */
  val count: ZIO[ZConnectionPool, Throwable, Option[Int]] = transaction {
    selectOne(
      sql"SELECT COUNT(*) FROM games".as[Int]
    )
  }

  /**
   * Retrieves the latest game between the specified home and away teams.
   *
   * @param homeTeam The home team
   * @param awayTeam The away team
   * @return The latest game as an option
   */
  def latest(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, Option[Game]] = {
    transaction {
      selectOne(
        sql"SELECT date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score, home_elo, away_elo, home_mlb, away_mlb, FROM games WHERE home_team = ${HomeTeam.unapply(homeTeam)} AND away_team = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 1".as[Game]
      )
    }
  }

  /**
   * Retrieves the game history of the specified team.
   *
   * @param team The team name
   * @return The game history as a chunk of games
   */
  def historyTeam(team: String): ZIO[ZConnectionPool, Throwable, zio.Chunk[Game]] = {
    transaction {
      selectAll(
        sql"SELECT date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score, home_elo, away_elo, home_mlb, away_mlb FROM games WHERE home_team = ${HomeTeam.unapply(HomeTeam(team))} OR away_team = ${AwayTeam.unapply(AwayTeam(team))}".as[Game]
      )
    }
  }

  /**
   * Retrieves the game history of the specified pitcher.
   *
   * @param pitcher The pitcher name
   * @return The game history as a chunk of games
   */
  def historyPitcher(pitcher: String): ZIO[ZConnectionPool, Throwable, zio.Chunk[Game]] = {
    transaction {
      selectAll(
        sql"SELECT date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score, home_elo, away_elo, home_mlb, away_mlb FROM games WHERE home_player = ${HomePlayer.unapply(HomePlayer(pitcher))} OR away_player = ${AwayPlayer.unapply(AwayPlayer(pitcher))}".as[Game]
      )
    }
  }

  /**
   * Retrieves the latest game of the specified team.
   *
   * @param team The team name
   * @return The latest game as an option
   */
  def realLatest(team: String): ZIO[ZConnectionPool, Throwable, Option[Game]] = {
    transaction {
      selectOne(
        sql"SELECT date, season_year, playoff_round, home_team, away_team, home_player, away_player, home_score, away_score, home_elo, away_elo, home_mlb, away_mlb FROM games WHERE home_team = ${HomeTeam.unapply(HomeTeam(team))} OR away_team = ${AwayTeam.unapply(AwayTeam(team))} ORDER BY date DESC LIMIT 1".as[Game]
      )
    }
  }

  /**
   * Retrieves the number of victories and defeats of the specified team in the given season.
   *
   * @param team The team name
   * @param season The season year
   * @return The number of victories and defeats as an option of ZResultSet
   */
  def victoriesAndDefeats(team: String, season: SeasonYear): ZIO[ZConnectionPool, Throwable, Option[zio.jdbc.ZResultSet]] = {
    transaction {
      selectOne(
        sql"SELECT (SELECT COUNT(*) FROM games WHERE season_year = ${SeasonYear.unapply(season)} AND ((home_team = ${HomeTeam.unapply(HomeTeam(team))} AND home_score > away_score) OR(away_team = ${HomeTeam.unapply(HomeTeam(team))} AND away_score > home_score))) AS nb_victory,(SELECT COUNT(*) FROM games     WHERE season_year = ${SeasonYear.unapply(season)} AND ((home_team = ${HomeTeam.unapply(HomeTeam(team))} AND home_score < away_score) OR (away_team = ${HomeTeam.unapply(HomeTeam(team))} AND away_score < home_score))) AS nb_defeat;"
      )
    }
  }
}
