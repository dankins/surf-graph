package com.surf.graph.rexster

import com.surf.graph.RawGraph
import com.tinkerpop.blueprints.impls.rexster.RexsterGraph
import com.tinkerpop.gremlin.scala.ScalaGraph

trait RexsterRawGraph extends RawGraph {
  val rexsterHost : String
  val rexsterPort : String
  val rexsterGraph : String
  lazy val rexsterUri = "http://" + rexsterHost + ":" + rexsterPort + "/graphs/" + rexsterGraph
  lazy val rawGraph : ScalaGraph = new RexsterGraph(rexsterUri)
}