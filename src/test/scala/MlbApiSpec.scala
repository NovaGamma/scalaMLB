package mlb

import munit._
import zio._
import zio.http._
import zio.jdbc._


class MlbApiSpec extends munit.ZSuite {

  val app: App[Any] = MlbApi.static
  val endpoints: Http[ZConnectionPool, Response, Request, Response] = MlbApi.endpoints


  testZ("Static route /text : should be ok ") {
    val req = Request.get(URL(Root / "text"))
    assertZ(app.runZIO(req).isSuccess)
  }

  testZ("Static route Json : should be ok") {
    val req = Request.get(URL(Root / "json"))
    assertZ(app.runZIO(req).isSuccess)
  }

  testZ("should be Not Found") {
    val req = Request.get(URL(Root))
    assertZ(endpoints.runZIO(req).isFailure)
  }

  testZ("should be initialized") {
    val req = Request.get(URL(Root / "init"))
    assertZ(endpoints.runZIO(req).isSuccess)
    assert(endpoints.runZIO(req).map(_.status.code == Status.NotImplemented.code))

  }
  
  testZ("should be games latest") {
    val homeTeam = "ATL"
    val awayTeam = "NYM"
    val req = Request.get(URL(Root / "game" / "latest" / homeTeam / awayTeam))

    val result = endpoints.runZIO(req)
    
    assertZ(result.isSuccess)
    assertZ(result.map(_.status.code == Status.Ok.code))
  }

  testZ("should be games predict") {
    val homeTeam = "ATL"
    val awayTeam = "NYM"
    val req = Request.get(URL(Root / "game" / "predict" / homeTeam / awayTeam))

    val result = endpoints.runZIO(req)
    
    assertZ(result.isSuccess)
    assertZ(result.map(_.status.code == Status.Ok.code))
  } 

  testZ("should be games count") {
    val req = Request.get(URL(Root / "games" / "count"))
    val result = endpoints.runZIO(req)
    
    assertZ(result.isSuccess)
    assertZ(result.map(_.status.code == Status.Ok.code))
  }

  testZ("should be No game in historical data") {
    val req = Request.get(URL(Root / "games" / "count_fail"))
    assertZ(endpoints.runZIO(req).isSuccess)
    assertZ(endpoints.runZIO(req).map(_.status).map(_.code).map(_ == 204))
  }

  testZ("should be game history") {
    val homeTeam = "ATL"
    val req = Request.get(URL(Root / "games" / "history" / homeTeam))
    val result = endpoints.runZIO(req)
    
    assertZ(result.isSuccess)
    assertZ(result.map(_.status.code == Status.Ok.code))
  } 

  testZ("should be pitcher history") {
    val pitcher = "German Marquez"
    val req = Request.get(URL(Root / "pitcher" / "history" / pitcher))
    val result = endpoints.runZIO(req)
    
    assertZ(result.isSuccess)
    assertZ(result.map(_.status.code == Status.Ok.code))
  } 
}