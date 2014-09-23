package models

import com.surf.graph.GraphAPIModule
import com.tinkerpop.blueprints.Direction
import play.api.libs.json.{Reads, JsValue, Writes, Json}


trait GraphJsonWrites {
  this: GraphAPIModule =>
  import objects._

  implicit def writesId : Writes[idType]
  implicit def readsId : Reads[idType]

  implicit def writesEdge[E](implicit objWrites: Writes[E]) = new Writes[GraphEdge[E]] {
    def writes(value: GraphEdge[E]): JsValue = Json.obj("id" -> value.id, "label" -> value.label, "object"->value.obj)
  }

  implicit def writesVertex[E](implicit objWrites: Writes[E]) = new Writes[GraphVertex[E]]{
    def writes(value: GraphVertex[E]): JsValue = Json.obj("id"->value.id,"type"->value.objType,"class"->value.objClass,"object"->value.obj)
  }

  implicit def writesEdgeTuple[E,V](implicit edgeWrites: Writes[E], vertexWrites : Writes[V]) = new Writes[EdgeTuple[E,V]] {
    def writes(value: EdgeTuple[E,V]): JsValue = Json.obj("link" -> value.edge, "connection" -> value.vertex, "direction" -> value.direction.toString)
  }

  implicit def writesSegment[V1,E,V2](implicit v1Writes : Writes[V1], edgeWrites: Writes[E], v2Writes : Writes[V2]) = new Writes[Segment[V1,E,V2]] {
    def writes(segment: Segment[V1,E,V2]): JsValue =
      if(segment.direction.equals(Direction.OUT))
        Json.obj("out" -> segment.v1, "in" -> segment.v2, "edge"->segment.edge)
      else
        Json.obj("out" -> segment.v2, "in" -> segment.v1, "edge"->segment.edge)

  }
}
