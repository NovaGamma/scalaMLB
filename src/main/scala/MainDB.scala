
package mlb

import zio._
import zio.jdbc._
import zio.http._
import zio.stream._
import com.github.tototoshi.csv._

object MlbApi extends ZIOAppDefault {

    val createZIOPoolConfig: ULayer[ZConnectionPoolConfig] =
        ZLayer.succeed(ZConnectionPoolConfig.default)

    val properties: Map[String, String] = Map(
        "user" -> "postgres",
        "password" -> "postgres"
    )

    val connectionPool : ZLayer[ZConnectionPoolConfig, Throwable, ZConnectionPool] =
        ZConnectionPool.h2mem(
            database = "testdb",
            props = properties
        )

    val create: ZIO[ZConnectionPool, Throwable, Unit] = transaction {
      execute(sql"""
          CREATE TABLE IF NOT EXISTS Elo_ratings (
          id SERIAL PRIMARY KEY,
          team_id INTEGER, 
          game_id INTEGER, 
          rating_pre FLOAT,
          rating_prob FLOAT,
          rating_post FLOAT,
          FOREIGN KEY (team_id) REFERENCES Team(id),
          FOREIGN KEY (game_id) REFERENCES Game(id)
          );"""
      );
      execute(sql"""
          CREATE TABLE IF NOT EXISTS Pitcher_ratings (
          id SERIAL PRIMARY KEY,
          player_id INTEGER,
          game_id INTEGER,
          rgs FLOAT,
          adj FLOAT,
          FOREIGN KEY (game_id) REFERENCES Game(id),
          FOREIGN KEY (player_id) REFERENCES Player(id)
          );"""
      );
      execute(sql"""
          CREATE TABLE IF NOT EXISTS Mlb_ratings (
          id SERIAL PRIMARY KEY,
          team_id INTEGER, 
          game_id INTEGER, 
          rating_pre FLOAT,
          rating_prob FLOAT,
          rating_post FLOAT,
          FOREIGN KEY (team_id) REFERENCES Team(id),
          FOREIGN KEY (game_id) REFERENCES Game(id)
          );"""
      );
      execute(sql"""
          CREATE TABLE IF NOT EXISTS Game (
          id SERIAL PRIMARY KEY,
          date VARCHAR(255),
          season INTEGER,
          neutral BOOLEAN,
          playoff VARCHAR(255),
          team1_id INTEGER,
          team2_id INTEGER,
          pitcher1_id INTEGER,
          pitcher2_id INTEGER,
          score1 INTEGER,
          score2 INTEGER,
          FOREIGN KEY (team1_id) REFERENCES Team(id),
          FOREIGN KEY (team2_id) REFERENCES Team(id),
          FOREIGN KEY (pitcher1_id) REFERENCES Player(id),
          FOREIGN KEY (pitcher2_id) REFERENCES Player(id),  
          );"""
      );
      execute(sql"""
          CREATE TABLE IF NOT EXISTS Team_player (
          id SERIAL PRIMARY KEY,
          season INTEGER,
          team_id INTEGER,
          player_id INTEGER,
          FOREIGN KEY (team_id) REFERENCES Team(id),
          FOREIGN KEY (player_id) REFERENCES Player(id)
          );"""
      );
      execute(sql"""
          CREATE TABLE IF NOT EXISTS Player (
          id SERIAL PRIMARY KEY,
          name VARCHAR(255)
          );"""
      );
      execute(sql"""
          CREATE TABLE IF NOT EXISTS Team (
          id SERIAL PRIMARY KEY,
          abbreviation VARCHAR(255)
          );"""
      );
    }

    def insertRows(chunk: Chunk[CsvStructure]): ZIO[ZConnectionPool, Throwable, Unit] = transaction {
        val insertTeams = chunk
            .map(_.team1)
            .union(chunk.map(_.team2))
            .distinct
            .map(team => execute(sql"INSERT INTO Team (abbreviation) VALUES ($team);"))

        val insertPlayers = chunk
            .flatMap(csvRow =>
            List(
                csvRow.pitcher1,
                csvRow.pitcher2
            ).flatten
            )
            .distinct
            .map(player => execute(sql"INSERT INTO Player (name) VALUES ($player);"))

        val insertTeamPlayers = chunk.flatMap { csvRow =>
            val team1Players = csvRow.pitcher1.toList ++ csvRow.pitcher2.toList
            val team2Players = csvRow.pitcher1.toList ++ csvRow.pitcher2.toList

            (team1Players ++ team2Players).distinct.map { player =>
                execute(
                    sql"""
                    INSERT INTO Team_player (season, team_id, player_id)
                    SELECT ${csvRow.season}, t.id, p.id
                    FROM Team t
                    CROSS JOIN Player p
                    WHERE t.abbreviation = ${csvRow.team1} AND p.name = $player
                    """
                )
            }
        }

        val insertGames = chunk.map { csvRow =>
            execute(
                sql"""
                INSERT INTO Game (date, season, neutral, playoff, team1_id, team2_id, pitcher1_id, pitcher2_id, score1, score2)
                SELECT ${csvRow.date}, ${csvRow.season}, ${csvRow.neutral}, ${csvRow.playoff},
                t1.id, t2.id, p1.id, p2.id, ${csvRow.score1}, ${csvRow.score2}
                FROM Team t1
                JOIN Team t2 ON t2.abbreviation = ${csvRow.team2}
                JOIN Player p1 ON p1.name = ${csvRow.pitcher1.getOrElse("")}
                JOIN Player p2 ON p2.name = ${csvRow.pitcher2.getOrElse("")}
                WHERE t1.abbreviation = ${csvRow.team1}
                """
            )
        }

        val insertEloRatings = chunk.map { csvRow =>
            execute(
                sql"""
                INSERT INTO Elo_ratings (team_id, game_id, rating_pre, rating_prob, rating_post)
                SELECT t.id, g.id, ${csvRow.elo1_pre}, ${csvRow.elo_prob1}, ${csvRow.elo1_post.getOrElse(0.0)}
                FROM Team t
                JOIN Game g ON g.date = ${csvRow.date} AND g.team1_id = t.id
                WHERE t.abbreviation = ${csvRow.team1}
                """
            ) *> execute(
                sql"""
                INSERT INTO Elo_ratings (team_id, game_id, rating_pre, rating_prob, rating_post)
                SELECT t.id, g.id, ${csvRow.elo2_pre}, ${csvRow.elo_prob2}, ${csvRow.elo2_post.getOrElse(0.0)}
                FROM Team t
                JOIN Game g ON g.date = ${csvRow.date} AND g.team2_id = t.id
                WHERE t.abbreviation = ${csvRow.team2}
                """
            )
        }

        val insertMlbRatings = chunk.map { csvRow =>
            execute(
                sql"""
                INSERT INTO Mlb_ratings (team_id, game_id, rating_pre, rating_prob, rating_post)
                SELECT t.id, g.id, ${csvRow.rating1_pre.getOrElse(0.0)}, ${csvRow.rating_prob1}, ${csvRow.rating1_post.getOrElse(0.0)}
                FROM Team t
                JOIN Game g ON g.date = ${csvRow.date} AND g.team1_id = t.id
                WHERE t.abbreviation = ${csvRow.team1}
                """
            ) *> execute(
                sql"""
                INSERT INTO Mlb_ratings (team_id, game_id, rating_pre, rating_prob, rating_post)
                SELECT t.id, g.id, ${csvRow.rating2_pre.getOrElse(0.0)}, ${csvRow.rating_prob2}, ${csvRow.rating2_post.getOrElse(0.0)}
                FROM Team t
                JOIN Game g ON g.date = ${csvRow.date} AND g.team2_id = t.id
                WHERE t.abbreviation = ${csvRow.team2}
                """
            )
        }

        val insertPitcherRatings = chunk.flatMap { csvRow =>
            val pitcher1 = csvRow.pitcher1.toList
            val pitcher2 = csvRow.pitcher2.toList

            (pitcher1 ++ pitcher2).distinct.map { player =>
            execute(
                sql"""
                INSERT INTO Pitcher_ratings (player_id, game_id, rgs, adj)
                SELECT p.id, g.id, ${csvRow.pitcher1_rgs.getOrElse(0.0)}, ${csvRow.pitcher1_adj.getOrElse(0.0)}
                FROM Player p
                JOIN Game g ON g.date = ${csvRow.date} AND (g.pitcher1_id = p.id OR g.pitcher2_id = p.id)
                WHERE p.name = $player
                """
            )
            }
        }

        for {
            _ <- ZIO.collectAllPar(insertTeams)
            _ <- ZIO.collectAllPar(insertPlayers)
            _ <- ZIO.collectAllPar(insertTeamPlayers)
            _ <- ZIO.collectAllPar(insertGames)
            _ <- ZIO.collectAllPar(insertEloRatings)
            _ <- ZIO.collectAllPar(insertMlbRatings)
            _ <- ZIO.collectAllPar(insertPitcherRatings)
        } yield ()
    }

    def convertToCsvStructure(csvRow: Seq[String]): CsvStructure = {
        CsvStructure(
            date = Some(csvRow(0)),
            season = csvRow(1).toIntOption,
            neutral = csvRow(2).toIntOption,
            playoff = Some(csvRow(3)),
            team1 = Some(csvRow(4)),
            team2 = Some(csvRow(5)),
            elo1_pre = csvRow(6).toDoubleOption,
            elo2_pre = csvRow(7).toDoubleOption,
            elo_prob1 = csvRow(8).toDoubleOption,
            elo_prob2 = csvRow(9).toDoubleOption,
            elo1_post = csvRow(10).toDoubleOption,
            elo2_post = csvRow(11).toDoubleOption,
            rating1_pre = csvRow(12).toDoubleOption,
            rating2_pre = csvRow(13).toDoubleOption,
            pitcher1 = Some(csvRow(14)),
            pitcher2 = Some(csvRow(15)),
            pitcher1_rgs = csvRow(16).toDoubleOption,
            pitcher2_rgs = csvRow(17).toDoubleOption,
            pitcher1_adj = csvRow(18).toDoubleOption,
            pitcher2_adj = csvRow(19).toDoubleOption,
            rating_prob1 = csvRow(20).toDoubleOption,
            rating_prob2 = csvRow(21).toDoubleOption,
            rating1_post = csvRow(22).toDoubleOption,
            rating2_post = csvRow(23).toDoubleOption,
            score1 = csvRow(24).toIntOption,
            score2 = csvRow(25).toIntOption
        )
    }

    val app: ZIO[ZConnectionPool, Throwable, Unit] = for {
        _ <- Console.printLine("Creation of the table")
        conn <- create
        _ <- Console.printLine("Opening the csv file")
        source <- ZIO.succeed(CSVReader.open("mlb_elo_latest.csv"))
        _ <- Console.printLine("Convert csv to the structure & insertion of rows into the DB")
        stream <- ZStream
        .fromIterator[Seq[String]](source.iterator)
        .map[CsvStructure](convertToCsvStructure)
        .filter(_.season.isDefined) // avoid None in the row
        .grouped(100)
        .foreach(insertRows)
        _ <- ZIO.succeed(source.close())
        _ <- Console.print("Insertion of rows succeed")
        //res <- select
    } yield ()

    override def run: ZIO[Any, Throwable, Unit] =
        app.provide(createZIOPoolConfig >>> connectionPool)
}