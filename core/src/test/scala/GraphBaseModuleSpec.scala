import com.surf.graph.{InMemoryGraphConfigModule, GraphBaseModuleImpl}
import com.tinkerpop.blueprints.{Edge, Vertex}
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline
import org.specs2.specification.Scope
import org.specs2.time.NoTimeConversions

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.specs2._


class GraphBaseModuleSpec extends mutable.Specification with NoTimeConversions{

  trait TestContext extends Scope
    with GraphBaseModuleImpl
    with InMemoryGraphConfigModule

  "GraphBaseModuleSpec" should {

    "allow creation of a vertex" in new TestContext{
      val v = Await.result(graphBase.addV(), 30 seconds)
    }

    "allow creation of an edge" in  new TestContext{
      val v1 = Await.result(graphBase.addV(), 30 seconds)
      val v2 = Await.result(graphBase.addV(), 30 seconds)

      val e = Await.result(graphBase.addE(v1, v2, "test"), 30 seconds)
    }
    "let you query the graph" in new TestContext {
      val pipe = Await.result(
        for {
          v <- graphBase.addV()
          pipe <- graphBase.vertexQuery(new GremlinScalaPipeline[Vertex, Vertex].has("id", v.getId))
        } yield pipe
      , 30 seconds)

      pipe.toList().size must be equalTo 1
    }

    "allow updating a vertex property" in new TestContext {
      val query = new GremlinScalaPipeline[Vertex, Vertex].has("foo", "bar")

      val (pipe, v1) = Await.result(
        for {
          v1 <- graphBase.addV()
          v2 <- graphBase.addV()
          v1x <- graphBase.setVertexProperty(v1.getId.asInstanceOf[graphBase.idType], "foo", "bar")
          v2x <- graphBase.setVertexProperty(v2.getId.asInstanceOf[graphBase.idType], "foo", "baz")
          pipe <- graphBase.vertexQuery(query)
        } yield (pipe, v1)

        , 30 seconds)

      pipe.toList().map(_.getId.toString) must be equalTo List(v1.getId.toString)
    }
    "allow edge queries"  in new TestContext {
      def filter(id : String) = new GremlinScalaPipeline[Edge,Edge] .has("id",id)

      val (pipe, v1) = Await.result(
        for {
          v1 <- graphBase.addV()
          v2 <- graphBase.addV()
          e <- graphBase.addE(v1,v2,"test")
          e2 <- graphBase.addE(v1,v2,"test2")
          pipe <- graphBase.edgeQuery(filter(e.getId.toString))
        } yield (pipe, v1)
      , 30 seconds)

      pipe.toList().map(_.getLabel) must be equalTo List("test")
    }
  }
}