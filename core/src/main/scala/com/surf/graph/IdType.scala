package com.surf.graph

trait IdType {
  val objects : GraphObjects
}

trait LongIds extends IdType {
  val objects = new Object with GraphObjects {
    type idType = Long
    type serializedIdType = Long
    type edgeIdType = Long
    type serializedEdgeIdType = Long

    implicit def serializeVertexId(id : idType) : serializedIdType = id
    implicit def deserializeVertexId(id : serializedIdType) : idType = id

    implicit def serializeEdgeId(id : edgeIdType) : serializedEdgeIdType = id
    implicit def deserializeEdgeId(id : serializedEdgeIdType) : edgeIdType = id
  }
}

trait StringGraphIds extends IdType {
  val objects = new Object with GraphObjects {
    type idType = String
    type serializedIdType = String
    type edgeIdType = String
    type serializedEdgeIdType = String

    implicit def serializeVertexId(id : idType) : serializedIdType = id
    implicit def deserializeVertexId(id : serializedIdType) : idType = id

    implicit def serializeEdgeId(id : edgeIdType) : serializedEdgeIdType = id
    implicit def deserializeEdgeId(id : serializedEdgeIdType) : edgeIdType = id
  }
}