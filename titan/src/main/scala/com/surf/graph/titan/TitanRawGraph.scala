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
  override def transaction[T](desc : String) (f : Option[TransactionalGraph] => Future[T]): Future[T] = {
    val tx = titanGraph.newTransaction()
    val ts = new java.util.Date().getTime
    logger.info(s"Started transaction $ts - $desc")
    f(Some(tx)).transform(
        { success =>
          tx.commit()
          val t2 = new java.util.Date().getTime - ts
          logger.info(s"Committed transaction $ts - $desc - $t2 ms")
          success
        },
        { fail =>
          tx.rollback()
          val t2 = new java.util.Date().getTime - ts
          logger.error(s"Rolling back transaction $ts - $desc - $t2 ms")
          fail
        }
    )

  }
}

