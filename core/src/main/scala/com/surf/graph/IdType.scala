package com.surf.graph

trait IdType {
  val objects : GraphObjects
}

trait LongGraphIds extends IdType {
  val objects = new Object with GraphObjects {
    type idType = Long
    def stringToId(id : String) : idType = id.toLong
    def idToString(id : Long) : String = id.toString
  }
}

trait StringGraphIds extends IdType {
  val objects = new Object with GraphObjects {
    type idType = String
    def stringToId(id : String) : String = id
    def idToString(id : String) : String = id
  }
}