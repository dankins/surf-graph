package com.surf.graph.titan

import com.surf.graph._
import com.thinkaurelius.titan.core.SchemaViolationException

import com.tinkerpop.blueprints.Graph

import scala.concurrent.Future

trait TitanGraphMutationModule extends GraphMutationModuleImpl {
  this: GraphBaseModule with IdType =>

  override val graphMutation = new TitanGraphBase

  class TitanGraphBase extends GraphMutationImpl with StandardExecutionContext {
    import objects._
    override def addVertex(objectType : String, props : Map[String,Any])( implicit graph : Graph) : Future[RawVertex] = {
      super.addVertex(objectType,props).recover {
        case e : SchemaViolationException => throw new GraphObjectUniqueConstraintException(e.getMessage,e)
      }
    }
  }
}