package mlb

import zio._
import zio.jdbc._
import zio.http._
import zio.stream._

import java.time.LocalDate
import java.sql.Date
import com.github.tototoshi.csv._
import scala.util.Try

object MlbApi extends ZIOAppDefault {

    import DataService._
    import GameDates.*
    import PlayoffRounds.*
    import SeasonYears.*
    import HomeTeams.*
    import AwayTeams.*
    import HomeScores.*
    import AwayScores.*
    import HomePlayers.*
    import AwayPlayers.*

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
        } yield Game(date, sy, playoffRound, homeTeam, awayTeam, homePlayer, awayPlayer, hs, as)

    val app: ZIO[ZConnectionPool, Throwable, Unit] = for {
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
        _ <- ZIO.succeed(source.close())
        test <- latest(HomeTeam("CHW"), AwayTeam("DET"))
        _ <- Console.printLine(test)
        _ <- Console.print("Insertion of rows succeed")
    } yield ()

    override def run: ZIO[Any, Throwable, Unit] =
        app.provide(createZIOPoolConfig >>> connectionPool)
}