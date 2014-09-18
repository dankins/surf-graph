package com.surf.graph

case class ObjectNotFoundException(message : String) extends Throwable(message)
case class PersistenceException(message : String) extends Throwable(message)
case class GraphObjectUniqueConstraintException(message : String) extends Throwable(message)
case class UnexpectedResultsException(message : String) extends Throwable(message)