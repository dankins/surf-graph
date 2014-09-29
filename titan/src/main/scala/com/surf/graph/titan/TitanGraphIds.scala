package com.surf.graph.titan

import com.surf.graph.{IdType, GraphObjects}
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier

trait TitanGraphIds extends IdType {
  val objects = new Object with GraphObjects {
    type idType = Long
    type serializedIdType = Long
    type edgeIdType = RelationIdentifier
    type serializedEdgeIdType = String

    implicit def serializeVertexId(id : idType) : serializedIdType = id
    implicit def deserializeVertexId(id : serializedIdType) : idType = id

    implicit def serializeEdgeId(id : edgeIdType) : serializedEdgeIdType = id.toString
    implicit def deserializeEdgeId(id : serializedEdgeIdType) : edgeIdType = RelationIdentifier.parse(id)

  }
}




