import com.surf.graph.{EdgeHelper, ObjectNotFoundException, SimpleEdgeHelper, DefaultVertexHelper}
import com.surf.graph.titan._
import com.tinkerpop.blueprints.Vertex
import com.typesafe.scalalogging.LazyLogging
import org.specs2.specification.Scope
import org.specs2.time.NoTimeConversions

import scala.concurrent.Await
import scala.concurrent.duration._

import org.specs2._


class TitanGraphModuleSpec extends mutable.Specification with NoTimeConversions{

  trait TestContext extends Scope with TitanInMemoryGraphModule with LazyLogging {
    val mgmt = titanGraph.getManagementSystem
    val fieldA = mgmt.makePropertyKey("Sample:fieldA").dataType(classOf[String]).make()
    mgmt.buildIndex("fieldAIndex",classOf[Vertex]).addKey(fieldA).unique().buildCompositeIndex()
    mgmt.commit()
  }

  case class Sample(fieldA : String, fieldB : Long, fieldC : Boolean)
  object Sample {
    implicit object VertexHelper extends DefaultVertexHelper[Sample]{
      val objectType = "Sample"
      val uniqueFields = Seq("Sample:fieldA")
      def toMap(obj : Sample) : Map[String,Any] = {
        Map(
          "Sample:fieldA" -> obj.fieldA,
          "Sample:fieldB" -> obj.fieldB,
          "Sample:fieldC" -> obj.fieldC
        )
      }

      def toObject(props : Map[String,Any]) : Sample = {
        Sample(
          fieldA = props.getOrElse("Sample:fieldA",throw new Exception("no fieldA")).asInstanceOf[String],
          fieldB = props.getOrElse("Sample:fieldB",throw new Exception("no fieldB")).asInstanceOf[Long],
          fieldC = props.getOrElse("Sample:fieldC",throw new Exception("no fieldC")).asInstanceOf[Boolean]
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
      val v = Await.result(graph.create(Sample("create-1",1,fieldC = true)), 30 seconds)
      v.objType must be equalTo "Sample"
    }

    "allow creation of an edge" in new TestContext{
      val v1 = Await.result(graph.create(Sample("edgeCreateTest-1",1,fieldC = false)), 30 seconds)
      val v2 = Await.result(graph.create(Sample("edgeCreateTest-2",1,fieldC = true)), 30 seconds)

      val e = Await.result(graph.createSegment(v1, SampleEdge(), v2), 30 seconds)

      val testLong = Long.MinValue
      logger.info(s"TEST: ${testLong.getClass.getCanonicalName}")
      logger.info(s"VERTEX: ${e.v1.id} - ${e.v1.id.toString} - ${e.v1.id.getClass.getCanonicalName}")
      logger.info(s"EDGE: ${e.edge.id} - ${e.edge.id.toString} - ${e.edge.id.getClass.getCanonicalName}")
      e.edge.label must be equalTo "SampleEdge"
    }

    "let you select from the graph" in new TestContext{
      val v1 = Await.result(graph.create(Sample("query-1",1,fieldC = true)), 30 seconds)

      val result = Await.result(graph.select[Sample](v1.id)(_.as("foo")) , 30 seconds)

      result.id must be equalTo v1.id
      result.obj.fieldC must beTrue

    }

    "allow updating a vertex property" in new TestContext{

      val (result, theid) = Await.result(
        for {
          v1 <- graph.create(Sample("update-1",1,fieldC = true))
          v2 <- graph.create(Sample("update-2",1,fieldC = true))
          v1x <- graph.update(v1.copy(obj = v1.obj.copy(fieldA = "update-3",fieldC = false)))
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
          v1 <- graph.create(Sample("edgequery-1",1,fieldC = true))
          v2 <- graph.create(Sample("edgequery-2",1,fieldC = true))
          e <- graph.createSegment(v1,SampleEdge(),v2)
          e2 <- graph.createSegment(v1,SampleEdge(),v2)
          result <- graph.getEdge[SampleEdge](e.edge.id)
        } yield (result, v1)
      , 30 seconds)

      result.label must be equalTo "SampleEdge"
    }

  "allow edge deletion" in new TestContext {
    for {
      v1 <- graph.create(Sample("edgequery-delete1",1,fieldC = true))
      v2 <- graph.create(Sample("edgequery-delete2",1,fieldC = true))
      e <- graph.createSegment(v1,SampleEdge(),v2)
      e2 <- graph.delete(e.edge)
      result <- graph.getEdge[SampleEdge](e.edge.id)
    } yield (result, v1)
  }
  "allow vertex delete" in new TestContext {
    Await.result(for {
      v <- graph.create(Sample("vertex-delete1",1,fieldC = true))
      d <- graph.delete(v)
      z <- graph.get[Sample](v.id)
    } yield z, 30 seconds) must throwA[ObjectNotFoundException]
  }

    "allow you to get a segment between vertices" in new TestContext {
      val segment = Await.result(for {
        v1 <- graph.create(Sample("segment-v1", 1,fieldC = true))
        v2 <- graph.create(Sample("segment-v2", 1,fieldC = true))
        e <- graph.createSegment(v1, SampleEdge(), v2)
        x <- graph.getSegmentFromVertices[Sample, SampleEdge, Sample](v1.id, v2.id, implicitly[EdgeHelper[SampleEdge]].label)
      } yield x, 30 seconds)

      segment.isDefined must beTrue
    }
  }
}