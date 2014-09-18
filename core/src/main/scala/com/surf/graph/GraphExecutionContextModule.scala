package com.surf.graph

import scala.concurrent.ExecutionContext

/**
 * Some example Execution contexts:
 * Akka.system.dispatchers.lookup("graph-context")
 * scala.concurrent.ExecutionContext.Implicits.global
 * play.api.libs.concurrent.Execution.Implicits.defaultContext
 */
trait GraphQueryExecutionContext {
  val graphQueryExecutionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
}
trait StandardExecutionContext {
  val standardExecutionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
}