package com.surf.graph

import com.tinkerpop.blueprints.{TransactionalGraph, Vertex}
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline

import scala.concurrent.Future

/**
 * Created by dan on 7/7/14.
 */
trait GraphMutationModule extends StandardExecutionContext {
  this: GraphObjects =>
  val graphMutation : GraphMutation

  trait GraphMutation {
    implicit val executionContext = standardExecutionContext
    // Create
    def addVertex(objType : String, objClass : String, props : Map[String,Any],tx : Option[TransactionalGraph] = None) : Future[RawVertex]
    def addEdge(outId : idType, inId : idType, label : String, props : Map[String,Any] = Map[String,Any](),tx : Option[TransactionalGraph] = None  ) : Future[RawEdge]
    def addEdgeUnique(outId : idType, inId : idType, label : String, props : Map[String,Any] = Map[String,Any](),tx : Option[TransactionalGraph] = None  ) : Future[RawEdge]
    //def addEdge[O,E,I](out : GraphVertex[O], edge : E, in : GraphVertex[I] ) : Future[Segment[O,E,I]]

    // Update
    def updateVertex(vertexId : idType, props : Map[String,Any],tx : Option[TransactionalGraph] = None) : Future[RawVertex]
    def updateEdge(edgeId : idType, props : Map[String,Any],tx : Option[TransactionalGraph] = None) : Future[RawEdge]
    def updateVertexProperty(id : idType, fieldName : String, value : Any,tx : Option[TransactionalGraph] = None) : Future[String]
    def updateEdgeProperty(id : idType, fieldName : String, value : Any,tx : Option[TransactionalGraph] = None) : Future[String]

    // Delete
    def deleteVertex(vertexId : idType,tx : Option[TransactionalGraph] = None) : Future[String]
    def deleteEdge(edgeId : idType,tx : Option[TransactionalGraph] = None) : Future[String]
  }

}

trait GraphMutationModuleImpl extends GraphMutationModule with StandardExecutionContext {
  this : GraphObjects  with GraphBaseModule =>
  val graphMutation = new GraphMutationImpl

  class GraphMutationImpl extends GraphMutation {


    def addVertex(objectType : String, objectClass : String, props : Map[String,Any],tx : Option[TransactionalGraph] = None) : Future[RawVertex] = {
      val allProps = props ++ Map("type" -> objectType,"class" -> objectClass)

      graphBase.addV(tx).map { v =>
        val modifiedProps = graphBase.prepareProps(allProps)
        modifiedProps.foreach{ kv =>
          v.setProperty(kv._1,kv._2)
        }
        RawVertex(v.getId.asInstanceOf[idType],modifiedProps)
      }
    }
    def addEdge(outId : idType, inId : idType, label : String, props : Map[String,Any] = Map[String,Any](),tx : Option[TransactionalGraph] = None  ) : Future[RawEdge] = {

      for {
        outV <- graphBase.v(outId,tx)
        inV <- graphBase.v(inId,tx)
        edge <- graphBase.addE(outV,inV,label,tx)
        complete  <- graphBase.setProperties(edge,props,tx)
      } yield RawEdge(edge.getId.asInstanceOf[idType],label,props)
    }

    def addEdgeUnique(outId : idType, inId : idType, label : String, props : Map[String,Any] = Map[String,Any](),tx : Option[TransactionalGraph] = None  ) : Future[RawEdge] = {
      val query = new GremlinScalaPipeline[Vertex,Vertex]
        .has("id",outId)
        .out(label)
        .has("id",inId)

      graphBase.genericVertexQuery(query,tx)
      .flatMap { results =>
        if(results.size == 0) addEdge(outId,inId,label,props)
        else throw new GraphObjectUniqueConstraintException("Edge already exists")
      }
    }
    def updateVertex(vertexId : idType, props : Map[String,Any],tx : Option[TransactionalGraph] = None) : Future[RawVertex] = {
      // TODO this is very slow
      val modifiedProps = graphBase.prepareProps(props)
      val futures = modifiedProps.map(pair => graphBase.setVertexProperty(vertexId,pair._1,pair._2,tx))

      Future.sequence(futures).map(x => RawVertex(vertexId,modifiedProps))
    }
    def updateEdge(edgeId : idType, props : Map[String,Any],tx : Option[TransactionalGraph] = None) : Future[RawEdge] = {
      throw new Exception("not implemented")
    }
    def updateVertexProperty(id : idType, fieldName : String, value : Any,tx : Option[TransactionalGraph] = None) : Future[String] = Future {
      value match {
        case None => // don't set the property if None
        case _ => graphBase.setVertexProperty(id, fieldName,value,tx)
      }

      "Operation Successful"
    }
    def updateEdgeProperty(id : idType, fieldName : String, value : Any,tx : Option[TransactionalGraph] = None) : Future[String] = Future {
      value match {
        case None => // don't set the property if None
        case _ => graphBase.setEdgeProperty(id,fieldName,value,tx)

      }
      "Operation Successful"
    }
    def deleteVertex(vertexId : idType,tx : Option[TransactionalGraph] = None) : Future[String] = Future {
      graphBase.v(vertexId).map(graphBase.removeVertex(_,tx))
      "Operation Successful"
    }
    def deleteEdge(edgeId : idType,tx : Option[TransactionalGraph] = None) : Future[String] = Future {
      graphBase.e(edgeId).map(graphBase.removeEdge(_,tx))
      "Operation Successful"
    }

  }
}
