package com.surf.graph

case class ObjectNotFoundException(message : String, nestedException : Throwable = null) extends Throwable(message,nestedException)
case class PersistenceException(message : String, nestedException : Throwable = null) extends Throwable(message,nestedException)
case class GraphObjectUniqueConstraintException(message : String, nestedException : Throwable = null) extends Throwable(message,nestedException)
case class UnexpectedResultsException(message : String, nestedException : Throwable = null) extends Throwable(message,nestedException)