import com.surf.graph.{SimpleEdgeHelper, DefaultVertexHelper}
import com.surf.graph.titan._
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{Graph, Edge, Vertex}
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline
import com.typesafe.scalalogging.LazyLogging
import org.specs2.specification.Scope
import org.specs2.time.NoTimeConversions

import scala.concurrent.Await
import scala.concurrent.duration._

import org.specs2._


class IndexTests extends mutable.Specification with NoTimeConversions with LazyLogging{

  def setup(g : TitanGraph) = {
    logger.info("Begin Setup")
    val mgmt = g.getManagementSystem
    val fieldA = mgmt.makePropertyKey("Sample:fieldA").dataType(classOf[String]).make()
    mgmt.buildIndex("fieldAIndex",classOf[Vertex]).addKey(fieldA).unique().buildCompositeIndex()
    mgmt.commit()
    logger.info("Setup Complete")
  }

  val context = new Object with TitanInMemoryGraphModule
  setup(context.titanGraph)

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
  "TitanIndexTests" should {
    "query a single vertex" in {
      //context.graphBase.executeV(100)(_.has("Sample:fieldA","somevalue"))(context.titanGraph)
      success
    }
    "query vertices" in {
      logger.info("Starting 'query vertices'")
      //context.graphBase.executeRaw(_.V.has("Sample:fieldA","somevalue").toStream())(context.titanGraph)
      context.graphQuery.getByKey("Sample:fieldA","somevalue")(context.titanGraph)

      //new GremlinScalaPipeline[Graph,Graph].start(context.titanGraph).V.has("Sample:fieldA","somevalue").iterate()

      logger.info("Complete 'query vertices'")
      success
    }
  }
}