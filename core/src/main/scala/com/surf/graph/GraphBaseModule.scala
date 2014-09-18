package com.surf.graph

import java.io.InputStream

import com.tinkerpop.blueprints.util.io.graphson.GraphSONReader
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline
import com.tinkerpop.blueprints.{Graph, TransactionalGraph, Edge, Vertex}
import com.tinkerpop.pipes.util.structures.Row

import scala.concurrent.Future

/**
 * Module for retrieving raw objects from Graph. Does not convert to/from objects
 */
trait GraphBaseModule {
  this: GraphQueryExecutionContext =>
  val graphBase : GraphBase

  val helpers : GraphObjects


  //private lazy val rawGraph = graphConfig.initialize()
  //val rawGraph : ScalaGraph

  trait GraphBase {
    import helpers._
    type idType = helpers.idType

    implicit val executionContext = graphQueryExecutionContext
    //def query(query : GremlinScalaPipeline[Vertex,_], select : Seq[String]) : Future[GremlinScalaPipeline[Vertex,Row[_]]]
    def addV(tx : Option[TransactionalGraph] = None) : Future[Vertex]
    def addE(out : Vertex, in : Vertex, label : String, tx : Option[TransactionalGraph] = None) : Future[Edge]

    def v(id : idType, tx : Option[TransactionalGraph] = None) : Future[Vertex]
    def e(id : idType, tx : Option[TransactionalGraph] = None) : Future[Edge]

    def removeVertex(v : Vertex,tx : Option[TransactionalGraph] = None) : Future[Unit]
    def removeEdge(e : Edge,tx : Option[TransactionalGraph] = None) : Future[Unit]

    def setVertexProperty(id : idType, propName : String, value : Any,tx : Option[TransactionalGraph] = None) : Future[Unit]

    def setEdgeProperty(id : idType, propName : String, value : Any,tx : Option[TransactionalGraph] = None) : Future[Unit]
    def setProperties(edge : Edge, props : Map[String,Any],tx : Option[TransactionalGraph] = None) : Future[Edge]

    def vertexQuery(pipe : GremlinScalaPipeline[Vertex,Vertex],tx : Option[TransactionalGraph] = None) : Future[GremlinScalaPipeline[Vertex,Vertex]]
    def edgeQuery(pipe : GremlinScalaPipeline[Edge,Edge],tx : Option[TransactionalGraph] = None) : Future[GremlinScalaPipeline[Edge,Edge]]
    def genericVertexQuery[S,E](pipe : GremlinScalaPipeline[S,E],tx : Option[TransactionalGraph] = None) : Future[List[E]]
    def genericEdgeQuery[S,E](pipe : GremlinScalaPipeline[S,E],tx : Option[TransactionalGraph] = None) : Future[List[E]]


    def shutdown() : Unit
    def loadJson(is : InputStream) : Unit

    /**
     * Takes a Row[_] object and extracts the vertex and converts to a RawVertex
     * @param row - The Row[_] object returned from running .select(...) on a Pipeline
     * @param vertexName - the step that contains the vertices to convert. If your Pipeline has .as("foo") after the vertices then vertexName should be "foo"
     * @param propsName - the step that contains the properties for the vertex. If your Pipeline has .propertyMap.as("props") then propsName should be "props"
     * @return - Combines the vertex and props and returns a RawVertex
     */
    def rowToRawVertex(row : Row[_], vertexName : String, propsName : String) : RawVertex

    /**
     * This is more or less identical to rowToRawVertex except it works for Edges
     */
    def rowToRawEdge(row : Row[_], edgeName : String, propsName : String) : RawEdge

    /**
     * Takes the props from an Object and prepares them to be inserted into the graph
     * For example, it will remove props with Option = None and unfold Options with Some(val)
     * @param props
     * @return
    */
    def prepareProps(props : Map[String,Any]) : Map[String,Any]
  }

}
trait GraphBaseModuleImpl extends GraphBaseModule with GraphQueryExecutionContext {
  this: GraphConfigModule =>

  val graphBase = new GraphBaseImpl

  class GraphBaseImpl extends GraphBase {
    import helpers._

    def transactionOrBase(txOpt : Option[TransactionalGraph]) : Graph = txOpt.getOrElse(rawGraph)

    /**
     * @inheritdoc
     */
    /*
    def query(query : GremlinScalaPipeline[Vertex,_], select : Seq[String]) = Future {
      rawGraph.V.addPipe(query)
        .select(select : _*)
    }
  */
    /**
     * @inheritdoc
     */
    def addV(tx : Option[TransactionalGraph] = None) = Future {
      transactionOrBase(tx).addVertex(null)
    }


    /**
     * @inheritdoc
     */
    def addE(out: Vertex, in: Vertex, label: String,tx : Option[TransactionalGraph] = None) = Future(transactionOrBase(tx).addEdge(null,out, in, label))

    /**
     * @inheritdoc
     */
    def v(id: idType,tx : Option[TransactionalGraph] = None) = Future(transactionOrBase(tx).getVertex(id))

    /**
     * @inheritdoc
     */
    def e(id: idType,tx : Option[TransactionalGraph] = None) = Future(transactionOrBase(tx).getEdge(id))

    /**
     * @inheritdoc
     */
    def removeVertex(v: Vertex,tx : Option[TransactionalGraph] = None) = Future(transactionOrBase(tx).removeVertex(v))

    /**
     * @inheritdoc
     */
    def removeEdge(e: Edge,tx : Option[TransactionalGraph] = None) = Future(transactionOrBase(tx).removeEdge(e))

    /**
     * @inheritdoc
     */
    def setVertexProperty(id : idType, propName : String, value : Any,tx : Option[TransactionalGraph] = None) = Future {
      transactionOrBase(tx).getVertex(id).setProperty(propName,value)
    }

    def setEdgeProperty(id : idType, propName : String, value : Any,tx : Option[TransactionalGraph] = None) = Future{
      transactionOrBase(tx).getEdge(id).setProperty(propName,value)
    }
    /**
     * @inheritdoc
     */
    def setProperties(edge : Edge, props : Map[String,Any],tx : Option[TransactionalGraph] = None) = Future {
      props.map(kv => edge.setProperty(kv._1,kv._2))
      edge
    }

    /**
     * @inheritdoc
     */
    def vertexQuery(pipe : GremlinScalaPipeline[Vertex,Vertex],tx : Option[TransactionalGraph] = None) : Future[GremlinScalaPipeline[Vertex,Vertex]] = Future {
      rawGraph.V.addPipe(pipe)
    }
    def edgeQuery(pipe : GremlinScalaPipeline[Edge,Edge],tx : Option[TransactionalGraph] = None) : Future[GremlinScalaPipeline[Edge,Edge]] = Future {
      rawGraph.E.addPipe(pipe)
    }

    def genericVertexQuery[S,E](pipe : GremlinScalaPipeline[S,E],tx : Option[TransactionalGraph] = None) : Future[List[E]] = Future{
      rawGraph.V.addPipe(pipe).toList()
    }
    def genericEdgeQuery[S,E](pipe : GremlinScalaPipeline[S,E],tx : Option[TransactionalGraph] = None) : Future[List[E]] = Future{
      rawGraph.E.addPipe(pipe).toList()
    }
    /**
     * @inheritdoc
     */
    def rowToRawVertex(row : Row[_], vertexName : String, propsName : String) : RawVertex = {
      val v = row.getColumn(vertexName).asInstanceOf[Vertex]
      val props = row.getColumn(propsName).asInstanceOf[Map[String,Any]]
      RawVertex(v.getId.asInstanceOf[idType],props)
    }

    /**
     * @inheritdoc
     */
    def rowToRawEdge(row : Row[_], edgeName : String, propsName : String) : RawEdge = {
      val e = row.getColumn(edgeName).asInstanceOf[Edge]
      val props = row.getColumn(propsName).asInstanceOf[Map[String,Any]]
      RawEdge(e.getId.asInstanceOf[idType],e.getLabel,props)
    }

    def getPath(a : String, b : String, hops : Int) = {
      throw new UnsupportedOperationException
    }

    /**
     * @inheritdoc
     */
    def shutdown() : Unit = rawGraph.shutdown()

    def loadJson(is : InputStream) = GraphSONReader.inputGraph(rawGraph,is)

    def prepareProps(props : Map[String,Any]) : Map[String,Any] = {
      props.foldLeft(Map[String,Any]()) { (acc, cur) =>
        cur._2 match {
          case None => acc // do nothing
          case Some(optval) => acc  ++ Map(cur._1 -> optval)
          case _ => acc  ++ Map(cur._1 -> cur._2)
        }
      }
    }
  }
}