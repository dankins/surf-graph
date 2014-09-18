package com.surf.graph.rexster

import com.surf.graph.{StringIds, GraphConfigModule}
import com.tinkerpop.blueprints.impls.rexster.RexsterGraph
import com.tinkerpop.gremlin.scala.ScalaGraph

trait RexsterGraphConfigModule extends GraphConfigModule with StringIds {
  val rexsterHost : String
  val rexsterPort : String
  val rexsterGraph : String
  lazy val rexsterUri = "http://" + rexsterHost + ":" + rexsterPort + "/graphs/" + rexsterGraph
  lazy val rawGraph : ScalaGraph = new RexsterGraph(rexsterUri)
}