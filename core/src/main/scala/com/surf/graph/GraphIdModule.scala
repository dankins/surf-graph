package com.surf.graph


trait GraphIdType {
  type idType
  val helpers = new Object with GraphObjects
}

trait StringIds extends GraphIdType{
  type idType = String
}
trait LongIds extends GraphIdType{
  type idType = String
}
