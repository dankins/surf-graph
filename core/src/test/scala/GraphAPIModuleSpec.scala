import com.surf.graph._
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import org.specs2.time.NoTimeConversions

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object GraphAPIModuleSpec extends Specification with NoTimeConversions {
  trait TestContext extends Scope
    with SampleAPersistenceModule
    with InMemoryGraph
    with StringGraphIds
    with BothModule

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

  trait SampleAPersistenceModule {
    this: GraphAPIModule =>
    val component = SampleAPersistence

    object SampleAPersistence {
      import objects._
      def create(obj : Sample) : Future[GraphVertex[Sample]] = {
        graph.create(obj)
      }

      def get(id : idType) : Future[GraphVertex[Sample]] = {
        graph.get[Sample](id)
      }
    }
  }

  trait BothModule {
    this: GraphAPIModule with SampleAPersistenceModule =>
    val bothModTest = BothTest

    object BothTest {
      import objects._
      def create(obj : Sample) : Future[GraphVertex[Sample]] = {
        graph.create(obj)
      }
      def createB(obj : Sample) : Future[GraphVertex[Sample]] = {
        component.create(obj)
      }
    }
  }

  trait TestLongIdsModule {
    this: GraphModuleWithLongIds =>

    trait TestFoo {
      import objects._
      def get(id : Long) : Future[GraphVertex[Sample]] = graph.get[Sample](id)
    }
  }

  trait SampleBPersistenceModule {
    this: GraphModule =>
    val component = SampleBPersistence

    object SampleBPersistence {
      import objects._
      def create(obj : Sample) : Future[GraphVertex[Sample]] = {
        graph.create(obj)
      }

      def get(id : idType) : Future[GraphVertex[Sample]] = {
        graph.get[Sample](id)
      }
    }
  }

  "GraphAPIModule" should {
    "allow creation of objects" in new TestContext {
      val result = Await.result(component.create(Sample("foo",1)),30 seconds)
      result.obj.fieldA must be equalTo "foo"
    }
    "allow you to mix modules" in new TestContext {
      val result = Await.result(bothModTest.createB(Sample("foo",1)),30 seconds)
      result.obj.fieldA must be equalTo "foo"
    }
  }

}
