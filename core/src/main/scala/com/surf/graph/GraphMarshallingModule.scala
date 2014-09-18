package com.surf.graph

import com.tinkerpop.blueprints.{Edge, Vertex}
import com.tinkerpop.pipes.util.structures.Row

import scala.util.Try


/**
 * Module to convert raw objects to rich case classes
 */
trait GraphMarshallingModule {
  val graphMarshalling : GraphMarshalling

  val helpers : GraphObjects

  trait GraphMarshalling {
    import helpers._
    /**
     * Converts a RawSegment to a Segment using the specified implicit Vertex and Edge Helpers
     */
    def castSegment[V1 : VertexHelper, E : EdgeHelper, V2 : VertexHelper](raw : RawSegment) : Segment[V1,E,V2]

    /**
     * Converts a RawVertex to a GraphVertex using an implicit VertexHelper
     */
    def castVertex[T : VertexHelper](v : RawVertex) : GraphVertex[T]

    /**
     * Converts a RawEdge to a GraphEdge using an implicit EdgeHelper
     */
    def castEdge[E : EdgeHelper](edge : RawEdge) : GraphEdge[E]

    /**
     * Converts a RawEdgeTuple of Edge+Vertex to a EdgeTuple using implicit Edge + Vertex helpers
     */
    def rawEdgeTuple[E : EdgeHelper,V : VertexHelper](tuple : RawEdgeTuple) : EdgeTuple[E,V]
  }
}


trait GraphMarshallingModuleImpl extends GraphMarshallingModule {
  val graphMarshalling = new GraphMarshallingImpl

  import helpers._

  class GraphMarshallingImpl extends GraphMarshalling {

    /**
     * @inheritdoc
     */
    def castSegment[V1 : VertexHelper, E : EdgeHelper, V2 : VertexHelper](raw : RawSegment) : Segment[V1,E,V2] = {
      //Logger.info(raw.v1.toString)
      //Logger.info(raw.edge.toString)
      //Logger.info(raw.v2.toString)
       Segment(
         v1 = castVertex[V1](raw.v1),
         edge = castEdge[E](raw.edge),
         v2 = castVertex[V2](raw.v2),
         direction = raw.direction
       )
    }

    /**
     * @inheritdoc
     */
    def castVertex[T : VertexHelper](v : RawVertex) : GraphVertex[T] = {
      val obj =Try {
         implicitly[VertexHelper[T]].toObject(v.props)
      }.recover{
        case e : NoSuchElementException => throw new Exception(s"could not convert to ${implicitly[VertexHelper[T]].objectType} with props: ${v.props.toString()}")
      }


      GraphVertex(
        id = v.id,
        objType =  v.props.get("type").get.toString,
        objClass =  v.props.get("class").get.toString,
        obj = obj.get
      )
    }

    /**
     * @inheritdoc
     */
    def castEdge[E : EdgeHelper](edge : RawEdge) : GraphEdge[E] = {
      helpers.toGraphEdge[E](edge)
    }

    /**
     * @inheritdoc
     */
    def rawEdgeTuple[E : EdgeHelper,V : VertexHelper](tuple : RawEdgeTuple) : EdgeTuple[E,V] = {
      EdgeTuple(
        vertex = castVertex[V](tuple.vertex),
        edge   = castEdge[E](tuple.edge),
        direction = tuple.direction
      )
    }


  }
}