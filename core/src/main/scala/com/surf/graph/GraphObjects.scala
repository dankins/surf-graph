package com.surf.graph

import com.tinkerpop.blueprints.Direction



trait StringIds extends GraphObjects {
  type idType = String
}
trait LongIds extends GraphObjects {
  type idType = Long
  // TODO I THINK TITAN STORES vertex as Long but Edge as String
}


trait GraphObjects {
  type idType

  case class GraphVertex[+T] (id : idType, objType : String, objClass : String, obj : T)
  case class GraphEdge[+T] (id : idType, label : String, obj : T)

  case class EdgeTuple[E,V](edge : GraphEdge[E], vertex : GraphVertex[V], direction : Direction)
  case class VertexEdges[V1,E,V2](v : GraphVertex[V1], items : Seq[EdgeTuple[E,V2]])

  case class Segment[V1,E,V2](v1 : GraphVertex[V1], edge : GraphEdge[E], v2 : GraphVertex[V2], direction : Direction)

  case class RawVertex(id : idType, props : Map[String,Any])
  case class RawEdge(id : idType, label : String, props : Map[String,Any])

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
    val objClass = v.props.get("class").get.toString

    GraphVertex(
      id = v.id,
      objType = objectType,
      objClass = objClass,
      obj = obj
    )
  }
}


/*
case class GraphVertex[+T] (id : String, objType : String, objClass : String, obj : T)
case class GraphEdge[+T] (id : String, label : String, obj : T)

case class RawVertex(id : String, props : Map[String,Any])
case class RawEdge(id : String, label : String, props : Map[String,Any])

case class RawEdgeTuple(edge : RawEdge, vertex : RawVertex, direction : Direction)
case class RawVertexEdges(v : RawVertex, items : Seq[RawEdgeTuple])
//case class RawSegmentX(out : RawVertex, edge : RawEdge, in : RawVertex)
case class RawSegment(v1 : RawVertex, edge : RawEdge, v2 : RawVertex, direction : Direction)

case class EdgeTuple[E,V](edge : GraphEdge[E], vertex : GraphVertex[V], direction : Direction)

case class VertexEdges[V1,E,V2](v : GraphVertex[V1], items : Seq[EdgeTuple[E,V2]])
//case class SegmentX[O,E,I](out : GraphVertex[O], edge : GraphEdge[E], in : GraphVertex[I])
case class Segment[V1,E,V2](v1 : GraphVertex[V1], edge : GraphEdge[E], v2 : GraphVertex[V2], direction : Direction)
*/
