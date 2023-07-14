package mlb

import zio.http._
import zio._
import zio.jdbc._
import zio.stream._
import zio.test._
import zio.test.Assertion._
import io.lemonlabs.uri.{Url, QueryString}

import mlb.MlbApiSpec


class MlbApiSpec extends munit.FunSuite {

  val app: App[Any] = MlbApi.static
    test("Endpoint: /text") {
     val url = URL.decode("http://localhost:8080/text")
      val req = url match {
        case Right(value) => Request.get(value).body.asString
        case Left(exception) => ZIO.fail(exception)
      }
      println(s"req response: ${req.map(req => req.toArray)}")
      assertZIO(req)(equalTo("Hello MLB Fans!")) 
    }
  
}
