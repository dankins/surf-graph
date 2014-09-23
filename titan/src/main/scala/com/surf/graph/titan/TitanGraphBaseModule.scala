package com.surf.graph.titan

import java.io.InputStream

import com.surf.graph._
import com.thinkaurelius.titan.core.TitanVertex
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReader
import com.tinkerpop.blueprints.{Edge, Vertex, Graph, TransactionalGraph}
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline
import com.tinkerpop.pipes.util.structures.Row

import scala.concurrent.Future

trait TitanGraphBaseModule extends GraphBaseModuleImpl with GraphQueryExecutionContext {
  this: TitanRawGraph   =>

  override val graphBase = new TitanGraphBase

  class TitanGraphBase extends GraphBaseImpl {
    override def addE(out: Vertex, in: Vertex, label: String,tx : Option[TransactionalGraph] = None) = Future{

      val outTitan : TitanVertex = out.asInstanceOf[TitanVertex]
      val inTitan : TitanVertex = in.asInstanceOf[TitanVertex]
      transactionOrBase(tx).addEdge(null,outTitan, inTitan, label)
    }

  }
}