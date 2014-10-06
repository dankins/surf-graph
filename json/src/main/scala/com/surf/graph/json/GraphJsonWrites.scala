package com.surf.graph.json

import com.surf.graph.GraphAPIModule
import com.tinkerpop.blueprints.Direction
import play.api.libs.json._


trait GraphJsonWrites {
  this: GraphAPIModule =>
  import objects._

  implicit def writesVertexId : Writes[serializedIdType]
  implicit def writesEdgeId : Writes[serializedEdgeIdType]
  implicit def readsVertexId : Reads[serializedIdType]
  implicit def readsEdgeId : Reads[serializedEdgeIdType]

  implicit def writesVertexIdConv : Writes[idType] = new Writes[idType]{
    def writes(obj : idType) : JsValue = Json.toJson(obj : serializedIdType)
  }
  implicit def writesEdgeIdConv : Writes[edgeIdType] = new Writes[edgeIdType]{
    def writes(obj : edgeIdType) : JsValue = Json.toJson(obj : serializedEdgeIdType)
  }

  implicit def readsVertexIdConv : Reads[idType] = new Reads[idType]{
    def reads(json : JsValue) : JsResult[idType] = {
        json.validate[serializedIdType].map( x => x : idType ) // implicitly convert serialized id to actual ID type
    }
  }
  implicit def readsEdgeIdConv : Reads[edgeIdType] = new Reads[edgeIdType]{
    def reads(json : JsValue) : JsResult[edgeIdType] = {
      json.validate[serializedEdgeIdType].map( x => x : edgeIdType ) // implicitly convert serialized id to actual ID type
    }
  }

  implicit def writesEdge[E](implicit objWrites: Writes[E]) = new Writes[GraphEdge[E]] {
    def writes(value: GraphEdge[E]): JsValue = {
      val serializedId : serializedEdgeIdType = value.id
      Json.obj("id" -> serializedId, "label" -> value.label, "object"->value.obj)
    }
  }

  implicit def writesVertex[E](implicit objWrites: Writes[E]) = new Writes[GraphVertex[E]]{
    def writes(value: GraphVertex[E]): JsValue = {
      val serializedId : serializedIdType = value.id
      Json.obj("id"->serializedId,"type"->value.objType,"object"->value.obj)
    }
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
