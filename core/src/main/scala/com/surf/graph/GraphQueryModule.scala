package com.surf.graph

import com.tinkerpop.blueprints._
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * The main module used to query a Graph Database
 */
trait GraphQueryModule extends StandardExecutionContext with LazyLogging {
  this: GraphBaseModule with IdType =>

  val graphQuery : GraphQuery
  trait GraphQuery {
    import objects._
    implicit val executionContext = standardExecutionContext

    /**
     * getByKey with id = vertexId
     */
    def get(vertexId : idType)(implicit graph : Graph) : Future[RawVertex]
    /**
     * Retrieves the object with the given vertexId from the Graph
     * It will marshall the object from a property map to an object of type T
     * Wraps the object in a GraphVertex containing graph metadata
     */
    def getByKey(key : String, value : Any)(implicit graph : Graph) : Future[RawVertex]


    /**
     * TODO
     * @param edgeId - the ID of the vertex to start from
     * @return
     */
    def getEdge(edgeId : idType)(implicit graph : Graph) : Future[RawEdge]

    /**
     * getEdges
     * @param vertexId - the ID of the vertex to start from
     * @param edgeDir - the direction of the edges to retrieve
     * @param label - the label of the edge to filter on
     * @return
     */
    def getEdges(vertexId : idType, edgeDir : Direction, label : String)(implicit graph : Graph) : Future[Seq[RawEdgeTuple]]

    /**
     * Checks if the given object is valid and able to be persisted to the graph without error.
     * @return - returns the object back if valid
     */
    def validate[T](v : T)(implicit graph : Graph, vertexHelper : VertexHelper[T]) : Future[T]

    def query[S,E](pipe : GremlinScalaPipeline[Graph,Graph] => GremlinScalaPipeline[S,E])(implicit graph : Graph) : Future[Seq[E]]

    def queryV[S,E](vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[Seq[RawVertex]]
    def queryV[S,E](key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[Seq[RawVertex]]
    def queryE[S,E](edgeId : idType)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,Edge])(implicit graph : Graph) : Future[Seq[RawEdge]]
    def queryE[S,E](key : String, value : Any)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,Edge])(implicit graph : Graph) : Future[Seq[RawEdge]]
    def genericQueryV[E](vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E])(implicit graph : Graph) : Future[Seq[E]]
    def genericQueryV[E](key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E])(implicit graph : Graph) : Future[Seq[E]]
    def genericQueryE[E](edgeId : idType)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E])(implicit graph : Graph) : Future[Seq[E]]
    def genericQueryE[E](key : String, value : Any)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E])(implicit graph : Graph) : Future[Seq[E]]
    /**
     * Very similar to query, except the end pipeline expects only a single object and will throw an exception otherwise
     * @param pipe - the pipeline used to select the object
     */
    def select(vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[RawVertex]
    def select(key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[RawVertex]
    /**
     * TODO
     */
    def querySegments(direction : Direction, edgeLabel : String, key : String, value : Any)(filter: GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[Seq[RawSegment]]
    def querySegments(direction : Direction, edgeLabel : String, vertexId : idType)(filter: GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[Seq[RawSegment]]



/*
    /**
     * Similar to the query method, however after the objects are selected it will then retrieve incident edges
     * @param filter - the pipeline used to filter the objects
     * @param direction - the direction of the edges to retrieve
     * @tparam V1 - the type of the object that is the base object returned
     * @tparam E - the type of Edge that will be used to filter
     * @tparam V2 - the type of object that is at the other end of the edge
     * @return - returns a list of objects that match the initial filter, then a list containing the incident edges and the corresponding object
     */
    def queryEdges[V1 : VertexHelper,E : EdgeHelper,V2 : VertexHelper](filter: GremlinScalaPipeline[Vertex,Vertex], direction : Direction) : Future[Seq[VertexEdges[V1,E,V2]]]
*/


    //def getEdges(vertexId : String, edgeDir : Direction, label : String) : Future[Seq[RawEdgeTuple]]

    def getSegment(edgeId : idType)(implicit graph : Graph) : Future[RawSegment]

    /**
     * Retrieves an option with RawSegment that exists between two vertices
     * @param v1 - the ID of the outbound vertex
     * @param v2 - the ID of the inbound vertex
     * @param label - the label of the edge between the two vertices
     * @return
     */
    def getSegmentFromVertices(v1 : idType, v2 : idType, label : String)(implicit graph : Graph ) : Future[Option[RawSegment]]


    //def getComponents(base : GremlinScalaPipeline[Vertex,Vertex], components : Map[String,GremlinScalaPipeline[Vertex,Vertex]]) : Seq[(RawVertex,Map[String,Seq[(RawEdge,RawVertex)]])]
    def validateUpdate[T](vertex : GraphVertex[T])(implicit graph : Graph, vertexHelper : VertexHelper[T]) : Future[T]


  }
}

trait GraphQueryModuleImpl extends GraphQueryModule with StandardExecutionContext{
  this : GraphBaseModule with IdType =>

  val graphQuery = new GraphQueryImpl

  class GraphQueryImpl extends GraphQuery {
    import objects._
    /**
     * @inheritdoc
     */
    def get(vertexId : idType)(implicit graph : Graph) : Future[RawVertex] = {
      graphBase.queryV(vertexId)(
        _.as("vertex")
        .propertyMap.as("properties")
        .select("vertex","properties")
        .map(graphBase.rowToRawVertex(_,"vertex","properties"))
      ).map(onlyOne)
    }

    private def onlyOne(results : Seq[RawVertex]) : RawVertex = {
      if(results.size.equals(0)) throw new ObjectNotFoundException(s"could not find object")
      else if(results.size > 1) throw new UnexpectedResultsException(s"error retrieving object - found ${results.size} results when expecting 1")
      else results.head
    }

    /**
     * @inheritdoc
     */
    def getByKey(key : String, value : Any)(implicit graph : Graph) : Future[RawVertex] = {
      graphBase.queryV(key,value)(
        _.as("vertex")
        .propertyMap.as("properties")
        .select("vertex","properties")
        .map(graphBase.rowToRawVertex(_,"vertex","properties"))
      ).map(onlyOne)
    }

    /**
     * @inheritdoc
     */
    def getEdge(edgeId : idType)(implicit graph : Graph) : Future[RawEdge] = {
      val pipe = graphBase.queryE(edgeId){
        _.as("edge").propertyMap.as("edge-props")
        .back("edge")
        .select("edge","edge-props")
        .map { row =>
          graphBase.rowToRawEdge(row, "edge", "edge-props")
        }
      }

      pipe.map { result =>
        if(result.size.equals(1)) result.head
        else if(result.size.equals(0)) throw new ObjectNotFoundException(s"could not find edge with id#$edgeId")
        else if(result.size > 1) throw new UnexpectedResultsException(s"error retrieving edge id#$edgeId - found ${result.size} results when expecting 1")
        else throw new UnsupportedOperationException("retrieving graph edges is not supported yet")
      }
    }

    /**
     * @inheritdoc
     */
    def getSegment(edgeId : idType)(implicit graph : Graph) : Future[RawSegment] = {
      val segment = graphBase.queryE(edgeId) {
        _.as("edge").propertyMap.as("edge-props")
        .back("edge")
        .inV.as("in").propertyMap.as("in-props")
        .back("edge")
        .outV.as("out").propertyMap.as("out-props")
        .back("edge")
        .select("edge","edge-props","in","in-props","out","out-props")
        .map(graphBase.rowToRawSegment(_))
      }

      segment.map { result =>
        result.head
      }.recover {
        case e : NoSuchElementException => throw new ObjectNotFoundException("could not find segment with id " + edgeId)
      }
    }

    /**
     * @inheritdoc
     */
    def getSegmentFromVertices(v1 : idType, v2 : idType, label : String)(implicit graph : Graph ) : Future[Option[RawSegment]] = {

      val segment = graphBase.queryV(v1) { pipe =>
        pipe.as("out").propertyMap.as("out-props").back("out")
          .outE(label).as("edge").propertyMap.as("edge-props").back("edge")
          .inV.has("id",v2.toString).as("in").propertyMap.as("in-props")
          .select("edge","edge-props","in","in-props","out","out-props")
          .map(graphBase.rowToRawSegment(_))
      }

      segment.map { segments =>
        if(segments.size == 0) None
        else if (segments.size == 1) Some(segments.head)
        else throw new UnexpectedResultsException("Expected to find one segment, but found "+ segments.size)
      }
    }

    /**
     * @inheritdoc
     */
    def getEdges(vertexId : idType, edgeDir : Direction, label : String)(implicit graph : Graph) : Future[Seq[RawEdgeTuple]] = {
      def inPipe(pipe : GremlinScalaPipeline[Vertex,Vertex]) = {
        pipe.as("base")
          .inE(label)
          .as("edge").propertyMap.as("edge-props")
          .back("edge")
          .outV.as("vertex")
          .propertyMap.as("props")
      }


      def outPipe(pipe : GremlinScalaPipeline[Vertex,Vertex]) = {
        pipe.as("base")
          .outE(label)
          .as("edge").propertyMap.as("edge-props")
          .back("edge")
          .inV.as("vertex")
          .propertyMap.as("props")
      }

      def dirPipe(pipe : GremlinScalaPipeline[Vertex,Vertex]) = {
        if(edgeDir.equals(Direction.IN)) inPipe(pipe)
        else outPipe(pipe)
      }
      // TODO what about Direction.BOTH?

      def finalPipe(pipe : GremlinScalaPipeline[Vertex,Vertex]) = {
        dirPipe(pipe)
          .select("edge","edge-props","vertex","props")
          .map { row =>
          val edgeShell = graphBase.rowToRawEdge(row,"edge","edge-props")
          val vertexShell = graphBase.rowToRawVertex(row, "vertex", "props")

          RawEdgeTuple(edgeShell,vertexShell, edgeDir)
        }
      }

      graphBase.queryV(vertexId)(finalPipe)
    }

    /**
     * @inheritdoc
     */
    def querySegmentsPartial(direction : Direction, edgeLabel : String)(filter: GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])
      : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,RawSegment] =
    {

      //val base = filter.as("v1").propertyMap.as("v1-props").back("v1")

      def out(pipe : GremlinScalaPipeline[Vertex,Any]) : GremlinScalaPipeline[Vertex,Any] = {
          pipe
          .outE(edgeLabel).as("edges")
          .propertyMap.as("edges-props")
          .back("edges")
          .inV.as("v2")
          .propertyMap.as("v2-props")
          .back("v1")
      }

      def in(pipe : GremlinScalaPipeline[Vertex,Any]) : GremlinScalaPipeline[Vertex,Any] = {
        pipe
          .inE(edgeLabel).as("edges")
          .propertyMap.as("edges-props")
          .back("edges")
          .outV.as("v2")
          .propertyMap.as("v2-props")
          .back("v1")
      }

      def both = {
        throw new UnsupportedOperationException
      }

      { pipe : GremlinScalaPipeline[Vertex,Vertex] =>
        val base = filter(pipe).as("v1").propertyMap.as("v1-props").back("v1")

        val q = if(direction.equals(Direction.OUT)) out(base)
                else if(direction.equals(Direction.IN)) in(base)
                else both
        q.select("v1","v1-props","edges","edges-props","v2","v2-props","direction")
        .map{ row =>
          val v1 = graphBase.rowToRawVertex(row,"v1","v1-props")
          val edge = graphBase.rowToRawEdge(row,"edges","edges-props")
          val v2 = graphBase.rowToRawVertex(row,"v2","v2-props")
          val dir =
            if(! direction.equals(Direction.BOTH)) direction
            else row.getColumn("direction").asInstanceOf[Direction]

          RawSegment(v1,edge,v2,dir)
        }
      }
    }
    def querySegments(direction : Direction, edgeLabel : String, vertexId : idType)(filter: GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[Seq[RawSegment]] = {
      graphBase.queryV(vertexId)(querySegmentsPartial(direction,edgeLabel)(filter))
    }
    def querySegments(direction : Direction, edgeLabel : String, key : String, value : Any)(filter: GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[Seq[RawSegment]] = {
      graphBase.queryV(key,value)(querySegmentsPartial(direction,edgeLabel)(filter))
    }

    def query[S,E](pipe : GremlinScalaPipeline[Graph,Graph] => GremlinScalaPipeline[S,E])(implicit graph : Graph) : Future[Seq[E]] = {
      logger.warn("This query is risky - large graphs may not use indexes")
      graphBase.query(pipe)
    }

    def queryV[S,E](vertexId : idType)(filter : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[Seq[RawVertex]] = {
      graphBase.queryV(vertexId){ pipe =>
        filter(pipe).as("vertices").propertyMap.as("vert-props").select("vertices","vert-props").map(graphBase.rowToRawVertex(_,"vertices","vert-props"))
      }
    }
    def queryV[S,E](key : String, value : Any)(filter : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[Seq[RawVertex]] = {
      graphBase.queryV(key,value){ pipe =>
        filter(pipe).as("vertices").propertyMap.as("vert-props").select("vertices","vert-props").map(graphBase.rowToRawVertex(_,"vertices","vert-props"))
      }
    }
    def queryE[S,E](edgeId : idType)(filter : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,Edge])(implicit graph : Graph) : Future[Seq[RawEdge]] = {
      graphBase.queryE(edgeId){ pipe =>
        filter(pipe).as("edge").propertyMap.as("edge-props").select("edge","edge-props").map(graphBase.rowToRawEdge(_,"edge","edge-props"))
      }
    }
    def queryE[S,E](key : String, value : Any)(filter : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,Edge])(implicit graph : Graph) : Future[Seq[RawEdge]] = {
      graphBase.queryE(key,value){ pipe =>
        filter(pipe).as("edge").propertyMap.as("edge-props").select("edge","edge-props").map(graphBase.rowToRawEdge(_,"edge","edge-props"))
      }
    }
    def genericQueryV[E](vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E])(implicit graph : Graph) : Future[Seq[E]] = graphBase.queryV(vertexId)(pipe)
    def genericQueryV[E](key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E])(implicit graph : Graph) : Future[Seq[E]] = graphBase.queryV(key,value)(pipe)
    def genericQueryE[E](edgeId : idType)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E])(implicit graph : Graph) : Future[Seq[E]] = graphBase.queryE(edgeId)(pipe)
    def genericQueryE[E](key : String, value : Any)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E])(implicit graph : Graph) : Future[Seq[E]] = graphBase.queryE(key,value)(pipe)

    /**
     * @inheritdoc
     */
    def select(id : idType)(filter: GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[RawVertex] = {
      queryV(id)(filter).map(onlyOne)
    }

    def select(key : String, value : Any)(filter: GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex])(implicit graph : Graph) : Future[RawVertex] = {
      queryV(key,value)(filter).map(onlyOne)
    }

    def validate[T](obj : T)(implicit graph : Graph, vertexHelper : VertexHelper[T]) : Future[T] = Future {
      implicitly[VertexHelper[T]].validate(obj,GremlinScalaPipeline(graph).V)
    }.flatMap {
        case Success(x) => Future.successful(x)
        case Failure(e) => Future.failed(e)
    }

    def validateUpdate[T](vertex : GraphVertex[T])(implicit graph : Graph, vertexHelper : VertexHelper[T]) : Future[T] = Future {
      implicitly[VertexHelper[T]].validate(vertex.obj,GremlinScalaPipeline(graph).V)
    }.flatMap {
      case Success(x) => Future.successful(x)
      case Failure(e) => Future.failed(e)
    }
  }
}