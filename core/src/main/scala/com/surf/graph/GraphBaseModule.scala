package com.surf.graph

import java.io.InputStream

import com.tinkerpop.blueprints.{Direction, Edge, Vertex, Graph}
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReader
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline
import com.tinkerpop.pipes.util.structures.Row
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future


/**
 * Module for retrieving raw objects from Graph. Does not convert to/from objects
 */
trait GraphBaseModule {
  this: IdType =>
  val graphBase : GraphBase
  //private lazy val rawGraph = graphConfig.initialize()
  //val rawGraph : ScalaGraph

  trait GraphBase {
    import objects._

    //def query(query : GremlinScalaPipeline[Vertex,_], select : Seq[String]) : Future[GremlinScalaPipeline[Vertex,Row[_]]]
    def addV(implicit graph : Graph) : Future[Vertex]
    def addE(out : Vertex, in : Vertex, label : String)(implicit graph : Graph) : Future[Edge]

    def v(id : idType)(implicit graph : Graph) : Future[Vertex]
    def e(id : edgeIdType)(implicit graph : Graph) : Future[Edge]

    def removeVertex(v : Vertex)(implicit graph : Graph) : Future[Unit]
    def removeEdge(e : Edge)(implicit graph : Graph) : Future[Unit]

    def setVertexProperty(id : idType, propName : String, value : Any)(implicit graph : Graph) : Future[Unit]

    def setEdgeProperty(id : edgeIdType, propName : String, value : Any)(implicit graph : Graph) : Future[Unit]
    def setProperties(edge : Edge, props : Map[String,Any]) : Future[Edge]

    def query[S,E](pipe : GremlinScalaPipeline[Graph,Graph] => GremlinScalaPipeline[S,E])(implicit graph : Graph) : Future[Seq[E]]
    def queryV[S,E](vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E])(implicit graph : Graph) : Future[Seq[E]]
    def queryV[S,E](key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E])(implicit graph : Graph) : Future[Seq[E]]
    def queryE[S,E](edgeId : edgeIdType)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E])(implicit graph : Graph) : Future[Seq[E]]
    def queryE[S,E](key : String, value : Any)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E])(implicit graph : Graph) : Future[Seq[E]]

    def shutdown(implicit graph : Graph) : Unit
    def loadJson(is : InputStream)(implicit graph : Graph) : Unit

    def getEdgeId(edge : Edge) : edgeIdType
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
     * Takes a Row[_] object and extracts the in vertex, out vertex, and edge and converts to a RawVertex
     * @param row - The Row[_] object returned from running .select(...) on a Pipeline
     * @param outLabel - The label for the field containing the "out" vertex. Defaults to "out"
     * @param outProps - The label for the field containing the "out" properties. Defaults to "out-props"
     * @param edgeLabel - The label for the field containing the edge. Defaults to "edge"
     * @param edgeProps - The label for the field containing the edge properties. Defaults to "edge-props"
     *@param inLabel - The label for the field containing the "in" vertex. Defaults to "in"
     * @param inProps - The label for the field containing the "in" properties. Defaults to "in-props"
     * @return
     */
    def rowToRawSegment(row : Row[_], outLabel : String = "out", outProps : String = "out-props", edgeLabel : String = "edge", edgeProps  : String = "edge-props", inLabel : String = "in", inProps : String = "in-props") : RawSegment
    /**
     * Takes the props from an Object and prepares them to be inserted into the graph
     * For example, it will remove props with Option = None and unfold Options with Some(val)
     * @return
    */
    def prepareProps(props : Map[String,Any]) : Map[String,Any]
  }

}
trait GraphBaseModuleImpl extends GraphBaseModule with GraphQueryExecutionContext with LazyLogging {
  this: IdType =>

  val graphBase = new GraphBaseImpl

  class GraphBaseImpl extends GraphBase {
    import objects._
    implicit val executionContext = graphQueryExecutionContext

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
    def addV(implicit graph : Graph) = Future {
      logger.debug("Adding vertex")
      graph.addVertex(null)
    }


    /**
     * @inheritdoc
     */
    def addE(out: Vertex, in: Vertex, label: String)(implicit graph : Graph) = Future(graph.addEdge(null,out, in, label))

    /**
     * @inheritdoc
     */
    def v(id: idType)(implicit graph : Graph) = Future(graph.getVertex(id))

    /**
     * @inheritdoc
     */
    def e(id: edgeIdType)(implicit graph : Graph) = Future(graph.getEdge(id))

    /**
     * @inheritdoc
     */
    def removeVertex(v: Vertex)(implicit graph : Graph) = Future(graph.removeVertex(v))

    /**
     * @inheritdoc
     */
    def removeEdge(e: Edge)(implicit graph : Graph) = Future(graph.removeEdge(e))

    /**
     * @inheritdoc
     */
    def setVertexProperty(id : idType, propName : String, value : Any)(implicit graph : Graph) = Future {
      graph.getVertex(id).setProperty(propName,value)
    }

    def setEdgeProperty(id : edgeIdType, propName : String, value : Any)(implicit graph : Graph) = Future{
      graph.getEdge(id).setProperty(propName,value)
    }
    /**
     * @inheritdoc
     */
    def setProperties(edge : Edge, props : Map[String,Any]) = Future {
      props.map(kv => edge.setProperty(kv._1,kv._2))
      edge
    }

    /**
     * @inheritdoc
     */
    def rowToRawVertex(row : Row[_], vertexName : String, propsName : String) : RawVertex = {
      val v = row.getColumn(vertexName).asInstanceOf[Vertex]
      val props = row.getColumn(propsName).asInstanceOf[Map[String,Any]]
      RawVertex(v.getId.asInstanceOf[idType],props)
    }

    def getEdgeId(edge : Edge) : edgeIdType = edge.getId.asInstanceOf[edgeIdType]
    /**
     * @inheritdoc
     */
    def rowToRawEdge(row : Row[_], edgeName : String, propsName : String) : RawEdge = {
      val e = row.getColumn(edgeName).asInstanceOf[Edge]
      //val props = row.getColumn(propsName).asInstanceOf[java.util.HashMap[String,Any]].asScala.toMap
      val props = row.getColumn(propsName).asInstanceOf[Map[String,Any]]
      RawEdge(e.getId.asInstanceOf[edgeIdType],e.getLabel,props)
    }

    def rowToRawSegment(row : Row[_], outLabel : String = "out", outProps : String = "out-props", edgeLabel : String = "edge", edgeProps  : String = "edge-props", inLabel : String = "in", inProps : String = "in-props") : RawSegment = {
      val out = graphBase.rowToRawVertex(row, outLabel, outProps)
      val edge = graphBase.rowToRawEdge(row, edgeLabel, edgeProps)
      val in = graphBase.rowToRawVertex(row, inLabel, inProps)

      RawSegment(out,edge, in, Direction.OUT)
    }

    def getPath(a : String, b : String, hops : Int) = {
      throw new UnsupportedOperationException
    }

    def query[S,E](pipe : GremlinScalaPipeline[Graph,Graph] => GremlinScalaPipeline[S,E])(implicit graph : Graph) : Future[Seq[E]] = Future{
      pipe(GremlinScalaPipeline(graph)).toList()
    }

    def queryV[S,E](vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E])(implicit graph : Graph) : Future[Seq[E]] = Future{
      val v = graph.getVertex(vertexId)
      if(v == null) throw new ObjectNotFoundException(s"Could find vertex with id#$vertexId")
      pipe(new GremlinScalaPipeline[Vertex,Vertex].start(v)).toList()
    }
    def queryV[S,E](key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E])(implicit graph : Graph) : Future[Seq[E]] = Future{
      val vertices = graph.getVertices(key,value)
      pipe(GremlinScalaPipeline.fromElements(vertices)).toList()
    }
    /*
    def queryV[S,E](vertices : Iterable[Vertex])(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[S,E])(implicit graph : Graph) : Future[Seq[E]] = Future{
      pipe(GremlinScalaPipeline.fromElements(vertices)).toList()
    }
    */
    def queryE[S,E](edgeId : edgeIdType)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E])(implicit graph : Graph) : Future[Seq[E]] = Future{
      val e = graph.getEdge(edgeId)
      if(e == null) throw new ObjectNotFoundException(s"Could find edge with id#$edgeId")
      pipe(new GremlinScalaPipeline[Edge,Edge].start(e)).toList()
    }
    def queryE[S,E](key : String, value : Any)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E])(implicit graph : Graph) : Future[Seq[E]] = Future{
      val edges = graph.getEdges(key,value)
      pipe(GremlinScalaPipeline.fromElements(edges)).toList()
    }
    /*
    def queryE[X,E](edges : Iterable[Edge])(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[X,E])(implicit graph : Graph) : Future[Seq[E]] = Future{
      pipe(GremlinScalaPipeline.fromElements(edges)).toList()
    }
    */

    /**
     * @inheritdoc
     */
    def shutdown(implicit graph : Graph) : Unit = graph.shutdown()

    def loadJson(is : InputStream)(implicit graph : Graph) = GraphSONReader.inputGraph(graph,is)

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