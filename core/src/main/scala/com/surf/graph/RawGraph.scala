package com.surf.graph

import com.tinkerpop.blueprints.{Graph, TransactionalGraph}
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.gremlin.scala.ScalaGraph

import scala.concurrent.Future

trait RawGraph {
  val rawGraph : ScalaGraph
  implicit lazy val graphImplicit : Graph = rawGraph
  def transaction[T](f : Option[TransactionalGraph] => Future[T]) : Future[T] = f(None)
}

trait InMemoryRawGraph extends RawGraph with StringGraphIds {
  val rawGraph : ScalaGraph = new TinkerGraph()
}
trait InMemoryGraph extends GraphModuleImpl with InMemoryRawGraph

trait FileGraphRawGraph extends RawGraph with StringGraphIds {
  val graphFileLocation : String
  lazy val rawGraph : ScalaGraph = new TinkerGraph(graphFileLocation, TinkerGraph.FileType.GRAPHSON)
}
trait FileGraph extends GraphModuleImpl with FileGraphRawGraph



