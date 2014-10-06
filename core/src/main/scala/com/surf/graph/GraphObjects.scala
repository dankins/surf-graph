package com.surf.graph

import com.tinkerpop.blueprints.Direction

trait GraphObjects {
    type idType
    type serializedIdType
    type edgeIdType
    type serializedEdgeIdType

    implicit def serializeVertexId(id : idType) : serializedIdType
    implicit def deserializeVertexId(id : serializedIdType) : idType

    implicit def serializeEdgeId(id : edgeIdType) : serializedEdgeIdType
    implicit def deserializeEdgeId(id : serializedEdgeIdType) : edgeIdType

    case class GraphVertex[+T] (id : idType, objType : String, obj : T)
    case class GraphEdge[+T] (id : edgeIdType, label : String, obj : T)

    case class EdgeTuple[E,V](edge : GraphEdge[E], vertex : GraphVertex[V], direction : Direction)
    case class VertexEdges[V1,E,V2](v : GraphVertex[V1], items : Seq[EdgeTuple[E,V2]])

    case class Segment[V1,E,V2](v1 : GraphVertex[V1], edge : GraphEdge[E], v2 : GraphVertex[V2], direction : Direction)

    case class RawVertex(id : idType, props : Map[String,Any])
    case class RawEdge(id : edgeIdType, label : String, props : Map[String,Any])

    case class RawEdgeTuple(edge : RawEdge, vertex : RawVertex, direction : Direction)
    case class RawVertexEdges(v : RawVertex, items : Seq[RawEdgeTuple])
    //case class RawSegmentX(out : RawVertex, edge : RawEdge, in : RawVertex)
    case class RawSegment(v1 : RawVertex, edge : RawEdge, v2 : RawVertex, direction : Direction)

    def toGraphEdge[T : EdgeHelper](e : RawEdge) : GraphEdge[T] = {
      GraphEdge(e.id,e.label,implicitly[EdgeHelper[T]].toObj(e.props))
    }

    def toGraphVertex[T : VertexHelper](v : RawVertex) : GraphVertex[T] = {
      val obj = implicitly[VertexHelper[T]].toObject(v.props)
      val objectType = v.props.get("type").get.toString

      GraphVertex(
        id = v.id,
        objType = objectType,
        obj = obj
      )
    }

    def toGraphSegment[V1 : VertexHelper, E : EdgeHelper, V2 : VertexHelper](raw : RawSegment) : Segment[V1,E,V2] = {
      Segment(
        v1 = toGraphVertex[V1](raw.v1),
        edge = toGraphEdge[E](raw.edge),
        v2 = toGraphVertex[V2](raw.v2),
        direction = raw.direction
      )
    }

    def rawEdgeTuple[E : EdgeHelper,V : VertexHelper](tuple : RawEdgeTuple) : EdgeTuple[E,V] = {
      EdgeTuple(
        vertex = toGraphVertex[V](tuple.vertex),
        edge   = toGraphEdge[E](tuple.edge),
        direction = tuple.direction
      )
    }
}