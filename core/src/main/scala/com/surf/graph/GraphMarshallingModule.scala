package com.surf.graph

/**
 * Module to convert raw objects to rich case classes
 */
trait GraphMarshallingModule {
  this: GraphObjects =>
  val graphMarshalling : GraphMarshalling

  trait GraphMarshalling {
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
  this: GraphObjects =>
  val graphMarshalling = new GraphMarshallingImpl

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
    def castVertex[T : VertexHelper](v : RawVertex) : GraphVertex[T] = toGraphVertex(v)

    /**
     * @inheritdoc
     */
    def castEdge[E : EdgeHelper](edge : RawEdge) : GraphEdge[E] = toGraphEdge(edge)

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