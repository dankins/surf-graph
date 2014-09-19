import java.io.FileNotFoundException

import com.tinkerpop.blueprints.{Vertex, Direction}
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline
import com.surf.graph.{InMemoryGraphConfigModule, GraphQueryModuleImpl, GraphBaseModuleImpl}
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import org.specs2.time.NoTimeConversions


import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source

class GraphQueryModuleSpec extends Specification with NoTimeConversions {

  trait TestContext extends Scope
    with GraphQueryModuleImpl
    with InMemoryGraphConfigModule
    with GraphBaseModuleImpl
  {
    val fixtures = "/fixtures/sampleGraph.json"
    val is = getClass.getResourceAsStream("/fixtures/sampleGraph.json")
    //val is = ClassLoader.getSystemClassLoader.getResourceAsStream(fixtures)
    if(is == null) throw new FileNotFoundException("cannot load resource "+ fixtures)
    graphBase.loadJson(is)
  }

  "GraphQueryModule" should {
    "allow retrieval of a vertex" in new TestContext {
      val v = Await.result(graphQuery.get("1"), 30 seconds)
      v.id must be equalTo "1"
    }
    "allow retrieval of a vertex properties" in new TestContext {
      val v = Await.result(graphQuery.getByKey("name","marko"), 30 seconds)
      v.props.getOrElse("name","empty").asInstanceOf[String] must be equalTo "marko"
      v.id must be equalTo "1"
    }
    "get a segment" in new TestContext {
      val segment = Await.result(graphQuery.getSegment("7"), 30 seconds)
      segment.v1.id must be equalTo "1"
      segment.v2.id must be equalTo "2"
    }
    "get a segment from vertices" in new TestContext {
      val segment = Await.result(graphQuery.getSegmentFromVertices("1","2","knows"), 30 seconds)
      segment.isDefined must beTrue
      //segment.get.v1.id must be equalTo "1"
      //segment.get.v2.id must be equalTo "2"
    }
    "get edges" in new TestContext {
      val edges = Await.result(graphQuery.getEdges("1",Direction.OUT,"knows"), 30 seconds)

      edges.map(_.edge.label) must be equalTo Seq("knows","knows")
      edges.map(_.vertex.id) must be equalTo Seq("2","4")
    }
    "test foo" in new TestContext {
      val query = new GremlinScalaPipeline[Vertex,Vertex]
        .has("id","1")
        .as("queue")
        .out()
    }
  }
}