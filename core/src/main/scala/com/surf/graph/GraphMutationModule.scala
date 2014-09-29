package com.surf.graph

import com.tinkerpop.blueprints.Graph
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline

import scala.concurrent.Future

trait GraphMutationModule extends StandardExecutionContext {
  this: GraphBaseModule with IdType =>
  val graphMutation : GraphMutation

  trait GraphMutation {
    import objects._
    implicit val executionContext = standardExecutionContext
    // Create
    def addVertex(objType : String, objClass : String, props : Map[String,Any])( implicit graph : Graph) : Future[RawVertex]
    def addEdge(outId : idType, inId : idType, label : String, props : Map[String,Any] = Map[String,Any]())(implicit graph : Graph) : Future[RawEdge]
    def addEdgeUnique(outId : idType, inId : idType, label : String, props : Map[String,Any] = Map[String,Any]())(implicit graph : Graph) : Future[RawEdge]
    //def addEdge[O,E,I](out : GraphVertex[O], edge : E, in : GraphVertex[I] ) : Future[Segment[O,E,I]]

    // Update
    def updateVertex(vertexId : idType, props : Map[String,Any])(implicit graph : Graph) : Future[RawVertex]
    def updateEdge(edgeId : idType, props : Map[String,Any])(implicit graph : Graph) : Future[RawEdge]
    def updateVertexProperty(id : idType, fieldName : String, value : Any)(implicit graph : Graph) : Future[String]
    def updateEdgeProperty(id : idType, fieldName : String, value : Any)(implicit graph : Graph) : Future[String]

    // Delete
    def deleteVertex(vertexId : idType)(implicit graph : Graph) : Future[String]
    def deleteEdge(edgeId : idType)(implicit graph : Graph) : Future[String]
  }

}

trait GraphMutationModuleImpl extends GraphMutationModule with StandardExecutionContext {
  this: GraphBaseModule with IdType =>
  val graphMutation = new GraphMutationImpl

  class GraphMutationImpl extends GraphMutation {
    import objects._
    def addVertex(objectType : String, objectClass : String, props : Map[String,Any])( implicit graph : Graph) : Future[RawVertex] = {
      val allProps = props ++ Map("type" -> objectType,"class" -> objectClass)

      graphBase.addV.map { v =>
        val modifiedProps = graphBase.prepareProps(allProps)
        modifiedProps.foreach{ kv =>
          v.setProperty(kv._1,kv._2)
        }
        RawVertex(v.getId.asInstanceOf[idType],modifiedProps)
      }
    }
    def addEdge(outId : idType, inId : idType, label : String, props : Map[String,Any] = Map[String,Any]())(implicit graph : Graph) : Future[RawEdge] = {

      for {
        outV <- graphBase.v(outId)
        inV <- graphBase.v(inId)
        edge <- graphBase.addE(outV,inV,label)
        complete  <- graphBase.setProperties(edge,props)
      } yield RawEdge(edge.getId.asInstanceOf[idType],label,props)
    }

    def addEdgeUnique(outId : idType, inId : idType, label : String, props : Map[String,Any] = Map[String,Any]())(implicit graph : Graph) : Future[RawEdge] = {
      val query = GremlinScalaPipeline(graph).V
        .has("id",outId)
        .out(label)
        .has("id",inId)

      graphBase.queryV(outId){
        _.out(label).has("id",inId)
      }.flatMap { results =>
        if(results.size == 0) addEdge(outId,inId,label,props)(graph)
        else throw new GraphObjectUniqueConstraintException("Edge already exists")
      }
    }
    def updateVertex(vertexId : idType, props : Map[String,Any])(implicit graph : Graph) : Future[RawVertex] = {
      // TODO this is very slow
      val modifiedProps = graphBase.prepareProps(props)
      val futures = modifiedProps.map(pair => graphBase.setVertexProperty(vertexId,pair._1,pair._2))

      Future.sequence(futures).map(x => RawVertex(vertexId,modifiedProps))
    }
    def updateEdge(edgeId : idType, props : Map[String,Any])(implicit graph : Graph) : Future[RawEdge] = {
      throw new Exception("not implemented")
    }
    def updateVertexProperty(id : idType, fieldName : String, value : Any)(implicit graph : Graph) : Future[String] = Future {
      value match {
        case None => // don't set the property if None
        case _ => graphBase.setVertexProperty(id, fieldName,value)
      }

      "Operation Successful"
    }
    def updateEdgeProperty(id : idType, fieldName : String, value : Any)(implicit graph : Graph) : Future[String] = Future {
      value match {
        case None => // don't set the property if None
        case _ => graphBase.setEdgeProperty(id,fieldName,value)

      }
      "Operation Successful"
    }
    def deleteVertex(vertexId : idType)(implicit graph : Graph) : Future[String] = Future {
      graphBase.v(vertexId).map(graphBase.removeVertex)
      "Operation Successful"
    }
    def deleteEdge(edgeId : idType)(implicit graph : Graph) : Future[String] = Future {
      graphBase.e(edgeId).map(graphBase.removeEdge)
      "Operation Successful"
    }

  }
}
