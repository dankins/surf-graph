package com.surf.graph.titan

import com.surf.graph._
import com.thinkaurelius.titan.core.TitanVertex
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier
import com.tinkerpop.blueprints.{Edge, Vertex, Graph}
import com.tinkerpop.pipes.util.structures.Row

import scala.concurrent.Future

trait TitanGraphBaseModule extends GraphBaseModuleImpl with GraphQueryExecutionContext{
  this: TitanRawGraph   =>

  override val graphBase = new TitanGraphBase

  class TitanGraphBase extends GraphBaseImpl {
    override def addE(out: Vertex, in: Vertex, label: String)(implicit graph : Graph) = Future {

      val outTitan : TitanVertex = out.asInstanceOf[TitanVertex]
      val inTitan : TitanVertex = in.asInstanceOf[TitanVertex]
      graph.addEdge(null,outTitan, inTitan, label)
    }
  }
}