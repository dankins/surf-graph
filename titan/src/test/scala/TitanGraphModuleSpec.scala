import com.surf.graph.{SimpleEdgeHelper, DefaultVertexHelper}
import com.surf.graph.titan._
import com.tinkerpop.blueprints.{Edge, Vertex}
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline
import org.specs2.specification.Scope
import org.specs2.time.NoTimeConversions

import scala.concurrent.Await
import scala.concurrent.duration._

import org.specs2._


class TitanGraphModuleSpec extends mutable.Specification with NoTimeConversions{

  trait TestContext extends Scope with TitanInMemoryGraphModule {
    val mgmt = titanGraph.getManagementSystem
    val fieldA = mgmt.makePropertyKey("Sample:fieldA").dataType(classOf[String]).make()
    mgmt.buildIndex("fieldAIndex",classOf[Vertex]).addKey(fieldA).unique().buildCompositeIndex()
    mgmt.commit()
  }

  case class Sample(fieldA : String, fieldB : Long)
  object Sample {
    implicit object VertexHelper extends DefaultVertexHelper[Sample]{
      val objectType = "Sample"
      val uniqueFields = Seq("Sample:fieldA")
      def toMap(obj : Sample) : Map[String,Any] = {
        Map(
          "Sample:fieldA" -> obj.fieldA,
          "Sample:fieldB" -> obj.fieldB
        )
      }

      def toObject(props : Map[String,Any]) : Sample = {
        Sample(
          fieldA = props.getOrElse("Sample:fieldA",throw new Exception("no fieldA")).asInstanceOf[String],
          fieldB = props.getOrElse("Sample:fieldB",throw new Exception("no fieldB")).asInstanceOf[Long]
        )
      }
    }
  }

  case class SampleEdge()
  object SampleEdge {
    implicit object SampleEdgeHelper extends SimpleEdgeHelper[SampleEdge] {
      val label = "SampleEdge"
      def toObj(props: Map[String,Any]) = SampleEdge()
    }
  }


  "TitanGraphModuleSpec" should {


    "allow creation of a vertex" in new TestContext{
      val v = Await.result(graph.create(Sample("create-1",1)), 30 seconds)
      v.objType must be equalTo "Sample"
    }

    "allow creation of an edge" in new TestContext{
      val v1 = Await.result(graph.create(Sample("edgeCreateTest-1",1)), 30 seconds)
      val v2 = Await.result(graph.create(Sample("edgeCreateTest-2",1)), 30 seconds)

      val e = Await.result(graph.createSegment(v1, SampleEdge(), v2), 30 seconds)
      e.edge.label must be equalTo "SampleEdge"
    }

    "let you select from the graph" in new TestContext{
      val v1 = Await.result(graph.create(Sample("query-1",1)), 30 seconds)

      val result = Await.result(graph.select[Sample](v1.id)(_.out("foo")) , 30 seconds)

      result.id must be equalTo v1.id
    }

    "allow updating a vertex property" in new TestContext{

      val (result, theid) = Await.result(
        for {
          v1 <- graph.create(Sample("update-1",1))
          v2 <- graph.create(Sample("update-2",1))
          v1x <- graph.update(v1.copy(obj = v1.obj.copy(fieldA = "update-3")))
          result <- graph.getByKey[Sample]("Sample:fieldA", "update-3")
        } yield (result, v1.id)

        , 30 seconds)

      result.id must be equalTo theid
      result.obj.fieldA must be equalTo "update-3"
    }
    "allow edge queries"  in new TestContext {
      //def filter(id : String) = graph.getEdge(id)

      val (result, v1) = Await.result(
        for {
          v1 <- graph.create(Sample("edgequery-1",1))
          v2 <- graph.create(Sample("edgequery-2",1))
          e <- graph.createSegment(v1,SampleEdge(),v2)
          e2 <- graph.createSegment(v1,SampleEdge(),v2)
          result <- graph.getEdge[SampleEdge](e.edge.id)
        } yield (result, v1)
      , 30 seconds)

      result.label must be equalTo "SampleEdge"
    }

  }
}