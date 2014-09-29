import com.surf.graph.{StringGraphIds, InMemoryRawGraph, GraphBaseModuleImpl}
import org.specs2.specification.Scope
import org.specs2.time.NoTimeConversions

import scala.concurrent.Await
import scala.concurrent.duration._

import org.specs2._


class GraphBaseModuleSpec extends mutable.Specification with NoTimeConversions{

  trait TestContext extends Scope
    with GraphBaseModuleImpl
    with InMemoryRawGraph
    with StringGraphIds

  "GraphBaseModuleSpec" should {

    "allow creation of a vertex" in new TestContext{
      val v = Await.result(graphBase.addV, 30 seconds)
    }

    "allow creation of an edge" in  new TestContext{
      val v1 = Await.result(graphBase.addV, 30 seconds)
      val v2 = Await.result(graphBase.addV, 30 seconds)

      val e = Await.result(graphBase.addE(v1, v2, "test"), 30 seconds)
    }
    "let you query the graph" in new TestContext {
      val pipe = Await.result(
        for {
          v <- graphBase.addV
          pipe <- graphBase.queryV(v.getId.toString)(_.id)
        } yield pipe
      , 30 seconds)

      pipe.size must be equalTo 1
    }

    "allow updating a vertex property" in new TestContext {
      //logger.info("START")
      def query = graphBase.queryV("foo", "bar")(_.has("foo","bar"))
      //logger.info("START END")

      val (pipe, v1) = Await.result(
        for {
          v1 <- graphBase.addV
          v2 <- graphBase.addV
          v1x <- graphBase.setVertexProperty(v1.getId.toString, "foo", "bar")
          v2x <- graphBase.setVertexProperty(v2.getId.toString, "foo", "baz")
          pipe <- query
        } yield (pipe, v1)

        , 30 seconds)

      pipe.map(_.getId.toString) must be equalTo List(v1.getId.toString)
    }
    "allow edge queries"  in new TestContext {

      val (pipe, v1) = Await.result(
        for {
          v1 <- graphBase.addV
          v2 <- graphBase.addV
          e <- graphBase.addE(v1,v2,"test")
          e2 <- graphBase.addE(v1,v2,"test2")
          pipe <- graphBase.queryE(e.getId.toString)(_.label)
        } yield (pipe, v1)
      , 30 seconds)

      pipe must be equalTo List("test")
    }
  }
}