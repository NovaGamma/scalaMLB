package mlb

import munit._
import zio._
import zio.http._
import zio.jdbc._


class MlbApiSpec extends munit.ZSuite {

  //Add the static routes and the endpoints from MlbApi
  val app: App[Any] = MlbApi.static
  val endpoints: Http[ZConnectionPool, Response, Request, Response] = MlbApi.endpoints


/**
  * Test the static /text endpoint of the app
  */
  testZ("Static route /text : should be ok ") {
    val req = Request.get(URL(Root / "text"))
    assertZ(app.runZIO(req).isSuccess)
  }

  /**
   * // Test the static  /json endpoint of the app
  */
  testZ("Static route Json : should be ok") {
    val req = Request.get(URL(Root / "json"))
    assertZ(app.runZIO(req).isSuccess)
  }

  /**
    *  Test an endpoint that should return Not Found
    */
  testZ("should be Not Found") {
    val req = Request.get(URL(Root))
    assertZ(endpoints.runZIO(req).isFailure)
  }

  /**
   * Test the /init endpoint
   */
  testZ("should be initialized") {
    val req = Request.get(URL(Root / "init"))
    assertZ(endpoints.runZIO(req).isSuccess)
    assert(endpoints.runZIO(req).map(_.status.code == Status.NotImplemented.code))

  }
  /**
   * Test the /game/latest/{homeTeam}/{awayTeam} endpoint
   */
  testZ("should be games latest") {
    val homeTeam = "ATL"
    val awayTeam = "NYM"
    val req = Request.get(URL(Root / "game" / "latest" / homeTeam / awayTeam))

    val result = endpoints.runZIO(req)
    
    assertZ(result.isSuccess)
    assertZ(result.map(_.status.code == Status.Ok.code))
  }

  /**
    * Test the /game/predict/{homeTeam}/{awayTeam} endpoint
    */
  testZ("should be games predict") {
    val homeTeam = "ATL"
    val awayTeam = "NYM"
    val req = Request.get(URL(Root / "game" / "predict" / homeTeam / awayTeam))

    val result = endpoints.runZIO(req)
    
    assertZ(result.isSuccess)
    assertZ(result.map(_.status.code == Status.Ok.code))
  } 

  /**
    * Test the /games/count endpoint
    */
  testZ("should be games count") {
    val req = Request.get(URL(Root / "games" / "count"))
    val result = endpoints.runZIO(req)
    
    assertZ(result.isSuccess)
    assertZ(result.map(_.status.code == Status.Ok.code))
  }

  /**
    * Test an endpoint that returns no game in the historical data
    */
  testZ("should be No game in historical data") {
    val req = Request.get(URL(Root / "games" / "count_fail"))
    assertZ(endpoints.runZIO(req).isSuccess)
    assertZ(endpoints.runZIO(req).map(_.status).map(_.code).map(_ == 204))
  }

  /**
    * Test the /games/history/{homeTeam} endpoint
    */
  testZ("should be game history") {
    val homeTeam = "ATL"
    val req = Request.get(URL(Root / "games" / "history" / homeTeam))
    val result = endpoints.runZIO(req)
    
    assertZ(result.isSuccess)
    assertZ(result.map(_.status.code == Status.Ok.code))
  } 
  
  /**
  * Test the /pitcher/history/{pitcher} endpoint
  */
  testZ("should be pitcher history") {
    val pitcher = "German Marquez"
    val req = Request.get(URL(Root / "pitcher" / "history" / pitcher))
    val result = endpoints.runZIO(req)
    
    assertZ(result.isSuccess)
    assertZ(result.map(_.status.code == Status.Ok.code))
  } 
}