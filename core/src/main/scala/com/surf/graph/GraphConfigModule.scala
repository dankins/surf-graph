package com.surf.graph

import com.tinkerpop.blueprints.TransactionalGraph
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.gremlin.scala.ScalaGraph

import scala.concurrent.Future


trait GraphConfigModule {
  this: GraphObjects =>
  val rawGraph : ScalaGraph
  def transaction[T](f : Option[TransactionalGraph] => Future[T]) : Future[T] = f(None)
}

trait InMemoryGraphConfigModule extends GraphConfigModule with StringIds {
  val rawGraph : ScalaGraph = new TinkerGraph()

}

trait FileGraphConfigModule extends GraphConfigModule with StringIds {
  val graphFileLocation : String
  lazy val rawGraph : ScalaGraph = new TinkerGraph(graphFileLocation, TinkerGraph.FileType.GRAPHSON)

}

