package com.surf.graph.titan

import com.surf.graph.{LongIds, StorageBackends, GraphConfigModule}

import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.TransactionalGraph
import com.tinkerpop.gremlin.scala.ScalaGraph
import org.apache.commons.configuration.BaseConfiguration

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait TitanGraphConfigModule
  extends GraphConfigModule
  with LongIds
{
  val batch = false

  val conf : BaseConfiguration

  lazy val titanGraph = TitanFactory.open(conf)
  lazy val rawGraph : ScalaGraph = titanGraph

  override def transaction[T](f : Option[TransactionalGraph] => Future[T]) : Future[T] = {
    val tx = titanGraph.newTransaction()
    f(Some(tx)).map{result => tx.commit(); result}
  }
}

