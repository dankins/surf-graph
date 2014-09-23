import java.io.FileNotFoundException

import com.surf.graph._
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import org.specs2.time.NoTimeConversions

import scala.concurrent.Await
import scala.concurrent.duration._

class GraphMutationModuleSpec extends Specification with NoTimeConversions {

  trait TestContext extends Scope
  with GraphMutationModuleImpl
  with InMemoryRawGraph
  with StringGraphIds
  with GraphBaseModuleImpl
  {
    val fixtures = "fixtures/sampleGraph.json"
    val is = getClass.getResourceAsStream("/fixtures/sampleGraph.json")
    if(is == null) throw new FileNotFoundException("cannot load resource "+ fixtures)
    graphBase.loadJson(is)

  }

  "GraphMutationModule" should {
    "allow creation of a vertex" in new TestContext {
      val v = Await.result(graphMutation.addVertex("test","test",Map("foo"->"bar")), 30 seconds)
      val result = Await.result(graphBase.v(v.id), 30 seconds)


      result.getProperty[String]("foo") must be equalTo "bar"
      result.getProperty[String]("class") must be equalTo "test"
      result.getProperty[String]("type") must be equalTo "test"
    }

    "allow creation of a unique segment" in new TestContext {
      val edge = Await.result(graphMutation.addEdge("1","2","TEST",Map()), 30 seconds)
      edge.label must be equalTo "TEST"

      val error = Await.result(graphMutation.addEdgeUnique("1","2","TEST",Map()), 30 seconds) must throwA[GraphObjectUniqueConstraintException]
    }
  }
}