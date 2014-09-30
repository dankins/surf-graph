package com.surf.graph.titan

import com.surf.graph.RawGraph

import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.TransactionalGraph
import com.tinkerpop.gremlin.scala.ScalaGraph
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.configuration.BaseConfiguration

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait TitanRawGraph extends RawGraph with TitanGraphIds with LazyLogging
{
  val conf : BaseConfiguration

  lazy val titanGraph = TitanFactory.open(conf)
  lazy val rawGraph : ScalaGraph = titanGraph
  override def transaction[T](f : Option[TransactionalGraph] => Future[T]) : Future[T] = {
    val tx = titanGraph.newTransaction()
    val ts = new java.util.Date().getTime.toString
    logger.debug(s"Started transaction $ts")
    f(Some(tx)).transform(
        {result => logger.debug(s"Committed transaction $ts"); tx.commit(); result},
        {fail => logger.debug(s"Rolling back transaction $ts"); tx.rollback(); fail}
    )

  }
}

