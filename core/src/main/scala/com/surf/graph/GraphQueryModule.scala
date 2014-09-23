package com.surf.graph

import com.tinkerpop.blueprints.{TransactionalGraph, Edge, Direction, Vertex}
import com.tinkerpop.gremlin.Tokens.T
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline
import com.tinkerpop.pipes.util.Pipeline
import com.tinkerpop.pipes.util.structures.Row

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * The main module used to query a Graph Database
 */
trait GraphQueryModule extends StandardExecutionContext{
  this: GraphBaseModule with IdType =>

  val graphQuery : GraphQuery
  trait GraphQuery {
    import objects._
    implicit val executionContext = standardExecutionContext

    /**
     * getByKey with id = vertexId
     */
    def get(vertexId : idType, tx : Option[TransactionalGraph] = None) : Future[RawVertex]
    /**
     * Retrieves the object with the given vertexId from the Graph
     * It will marshall the object from a property map to an object of type T
     * Wraps the object in a GraphVertex containing graph metadata
     */
    def getByKey(key : String, value : Any, tx : Option[TransactionalGraph] = None) : Future[RawVertex]


    /**
     * TODO
     * @param edgeId
     * @return
     */
    def getEdge(edgeId : idType, tx : Option[TransactionalGraph] = None) : Future[RawEdge]

    /**
     * getEdges
     * @param vertexId
     * @param edgeDir
     * @param label
     * @return
     */
    def getEdges(vertexId : idType, edgeDir : Direction, label : String, tx : Option[TransactionalGraph] = None) : Future[Seq[RawEdgeTuple]]

    /**
     * Checks if the given object is valid and able to be persisted to the graph without error.
     * @return - returns the object back if valid
     */
    def validate[T : VertexHelper](v : T, tx : Option[TransactionalGraph] = None) : Future[T]

    /**
     * Filters using a graph pipeline and returns the matching vertex objects
     * @param filter - the pipeline used to filter the objects
     */
    def query(filter: GremlinScalaPipeline[Vertex,Vertex], tx : Option[TransactionalGraph] = None) : Future[Seq[RawVertex]]
    //def query(query : GremlinScalaPipeline[Vertex,_], select : Seq[String]) : Future[GremlinScalaPipeline[Vertex,Row[_]]]

    /**
     * TODO
     */
    def querySegments(filter: GremlinScalaPipeline[Vertex,Vertex], direction : Direction, edgeLabel : String, tx : Option[TransactionalGraph] = None) : Future[Seq[RawSegment]]

    /**
     * TODO
     */
    def genericVertexQuery[S,E](pipe : GremlinScalaPipeline[S,E], tx : Option[TransactionalGraph] = None) : Future[List[E]]

    /**
     * TODO
     */
    def genericEdgeQuery[S,E](pipe : GremlinScalaPipeline[S,E], tx : Option[TransactionalGraph] = None) : Future[List[E]]

    /**
     * Very similar to query, except the end pipeline expects only a single object and will throw an exception otherwise
     * @param filter - the pipeline used to select the object
     */
    def select(filter: GremlinScalaPipeline[Vertex,Vertex], tx : Option[TransactionalGraph] = None) : Future[RawVertex]
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

    def getSegment(edgeId : idType, tx : Option[TransactionalGraph] = None) : Future[RawSegment]

    /**
     * Retrieves an option with RawSegment that exists between two vertices
     * @param v1 - the ID of the outbound vertex
     * @param v2 - the ID of the inbound vertex
     * @param label - the label of the edge between the two vertices
     * @return
     */
    def getSegmentFromVertices(v1 : idType, v2 : idType, label : String, tx : Option[TransactionalGraph] = None ) : Future[Option[RawSegment]]


    //def getComponents(base : GremlinScalaPipeline[Vertex,Vertex], components : Map[String,GremlinScalaPipeline[Vertex,Vertex]]) : Seq[(RawVertex,Map[String,Seq[(RawEdge,RawVertex)]])]
    def validateUpdate[T : VertexHelper](vertex : GraphVertex[T], tx : Option[TransactionalGraph] = None) : Future[T]


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
    def get(vertexId : idType, tx : Option[TransactionalGraph] = None) : Future[RawVertex] = {
      getByKey("id",vertexId,tx)
    }

    /**
     * @inheritdoc
     */
    def getByKey(key : String, value : Any, tx : Option[TransactionalGraph] = None) : Future[RawVertex] = {
      val pipe = new GremlinScalaPipeline[Vertex,Vertex]
        .has(key,value)
      query(pipe).map { result =>
        if(result.size.equals(0)) throw new ObjectNotFoundException(s"could not find object with key $key and value ${value.toString}")
        else if(result.size > 1) throw new UnexpectedResultsException(s"error retrieving object with key $key and value ${value.toString} - found ${result.size} results when expecting 1")
        else result.head
      }
    }

    /**
     * @inheritdoc
     */
    def getEdge(edgeId : idType, tx : Option[TransactionalGraph] = None) : Future[RawEdge] = {
      val pipe = new GremlinScalaPipeline[Edge,Edge]
        .has("id",edgeId)
        .as("edge").propertyMap.as("edge-props")
        .back("edge")
        .select("edge","edge-props")
        .map { row =>
          graphBase.rowToRawEdge(row, "edge", "edge-props")
        }


      graphBase.genericEdgeQuery(pipe,tx).map { result =>
        if(result.size.equals(1)) result.head
        else if(result.size.equals(0)) throw new ObjectNotFoundException(s"could not find edge with id#$edgeId")
        else if(result.size > 1) throw new UnexpectedResultsException(s"error retrieving edge id#$edgeId - found ${result.size} results when expecting 1")
        else throw new UnsupportedOperationException("retrieving graph edges is not supported yet")
      }
    }

    /**
     * @inheritdoc
     */
    def getSegment(edgeId : idType, tx : Option[TransactionalGraph] = None) : Future[RawSegment] = {
      val filter = new GremlinScalaPipeline[Edge,Edge]
        .has("id",edgeId)
        .as("edge").propertyMap.as("edge-props")
        .back("edge")
        .inV.as("in").propertyMap.as("in-props")
        .back("edge")
        .outV.as("out").propertyMap.as("out-props")
        .back("edge")
        .map(_.asInstanceOf[Edge])


      graphBase.genericEdgeQuery(getSegmentQuery(filter),tx).map { result =>
        result.head
      }.recover {
        case e : NoSuchElementException => throw new ObjectNotFoundException("could not find segment with id " + edgeId)
      }
    }

    /**
     * @inheritdoc
     */
    def getSegmentFromVertices(v1 : idType, v2 : idType, label : String, tx : Option[TransactionalGraph] = None ) : Future[Option[RawSegment]] = {

      val filter = new GremlinScalaPipeline[Vertex,Vertex]
        .has("id",v1.toString).as("out").propertyMap.as("out-props").back("out")
        .outE(label).as("edge").propertyMap.as("edge-props").back("edge")
        .inV.has("id",v2.toString).as("in").propertyMap.as("in-props")

      graphBase.genericVertexQuery(getSegmentQuery(filter),tx).map { segments =>
        if(segments.size == 0) None
        else if (segments.size == 1) Some(segments.head)
        else throw new UnexpectedResultsException("Expected to find one segment, but found "+ segments.size)
      }
    }

    def getSegmentQuery(filter : GremlinScalaPipeline[_,_]) : GremlinScalaPipeline[_,RawSegment] = {
      filter.select("edge","edge-props","in","in-props","out","out-props")
      .map { row =>
        val out = graphBase.rowToRawVertex(row, "out", "out-props")
        val edge = graphBase.rowToRawEdge(row,"edge","edge-props")
        val in = graphBase.rowToRawVertex(row, "in", "in-props")

        RawSegment(out,edge, in, Direction.OUT)
      }
    }

    /**
     * @inheritdoc
     */
    def getEdges(vertexId : idType, edgeDir : Direction, label : String, tx : Option[TransactionalGraph] = None) : Future[Seq[RawEdgeTuple]] = {
      val inPipe = new GremlinScalaPipeline[Vertex,Vertex]
        .has("id",vertexId).as("base")
        .inE(label)
        .as("edge").propertyMap.as("edge-props")
        .back("edge")
        .outV.as("vertex")
        .propertyMap.as("props")
        .back("vertex")

      val outPipe = new GremlinScalaPipeline[Vertex,Vertex]
        .has("id",vertexId).as("base")
        .outE(label)
        .as("edge").propertyMap.as("edge-props")
        .back("edge")
        .inV.as("vertex")
        .propertyMap.as("props")
        .back("vertex")

      val pipe = if(edgeDir.equals(Direction.IN)) inPipe
                 else outPipe
      // TODO what about Direction.BOTH?

      val finalPipe = pipe
        .select("edge","edge-props","vertex","props")
        .map { row =>
          val edgeShell = graphBase.rowToRawEdge(row,"edge","edge-props")
          val vertexShell = graphBase.rowToRawVertex(row, "vertex", "props")

          RawEdgeTuple(edgeShell,vertexShell, edgeDir)
        }
      graphBase.genericVertexQuery(finalPipe,tx)
    }

    /**
     * @inheritdoc
     */
    def query(filter: GremlinScalaPipeline[Vertex,Vertex], tx : Option[TransactionalGraph]) : Future[Seq[RawVertex]] = {
      val q = filter
        .as("vertices")
        .propertyMap.as("properties")
        .back("vertices")
        .map(_.asInstanceOf[Vertex])

      query(q,Seq("vertices","properties"),tx)
      .map { queryRow =>
        queryRow.map(graphBase.rowToRawVertex(_,"vertices","properties"))
          .toList()
      }
    }

    def query(query : GremlinScalaPipeline[Vertex,Vertex], select : Seq[String], tx : Option[TransactionalGraph]) : Future[GremlinScalaPipeline[Vertex,Row[_]]] = {
      graphBase.vertexQuery(query,tx)
        .map(_.select(select : _*))
    }

    def queryEdges(query : GremlinScalaPipeline[Edge,Edge], select : Seq[String], tx : Option[TransactionalGraph]) : Future[GremlinScalaPipeline[Edge,Row[_]]] = {
      graphBase.edgeQuery(query,tx)
        .map(_.select(select : _*))
    }

    def genericVertexQuery[S,E](pipe : GremlinScalaPipeline[S,E], tx : Option[TransactionalGraph] = None) : Future[List[E]] = {
      graphBase.genericVertexQuery(pipe,tx)
    }

    def genericEdgeQuery[S,E](pipe : GremlinScalaPipeline[S,E], tx : Option[TransactionalGraph] = None) : Future[List[E]] = {
      graphBase.genericEdgeQuery(pipe,tx)
    }
    /**
     * @inheritdoc
     */
    def querySegments(filter: GremlinScalaPipeline[Vertex,Vertex], direction : Direction, edgeLabel : String, tx : Option[TransactionalGraph] = None) : Future[Seq[RawSegment]] = {

      val base = filter.as("v1").propertyMap.as("v1-props").back("v1")
        .asInstanceOf[GremlinScalaPipeline[Vertex,Vertex]]
      def out : GremlinScalaPipeline[Vertex,Vertex] = {
          new GremlinScalaPipeline[Vertex,Vertex]
          .outE(edgeLabel).as("edges")
          .propertyMap.as("edges-props")
          .back("edges")
          .inV.as("v2")
          .propertyMap.as("v2-props")
          .back("v1")
          .map(_.asInstanceOf[Vertex])
      }

      def in : GremlinScalaPipeline[Vertex,Vertex] = {
        new GremlinScalaPipeline[Vertex,Vertex]
          .inE(edgeLabel).as("edges")
          .propertyMap.as("edges-props")
          .back("edges")
          .outV.as("v2")
          .propertyMap.as("v2-props")
          .back("v1")
          .map(_.asInstanceOf[Vertex])
      }

      def both = {
        throw new UnsupportedOperationException

        /*
        base.or(
          new GremlinScalaPipeline[Vertex,Vertex]
            .inE(edgeLabel).as("edges")
            .propertyMap.as("edges-props")
            .back("edges")
            .outV.as("v2")
            .propertyMap.as("v2-props")
            .map(x => Direction.IN).as("direction")
            .back("v2"),
          new GremlinScalaPipeline[Vertex,Vertex]
            .outE(edgeLabel).as("edges")
            .propertyMap.as("edges-props")
            .back("edges")
            .inV.as("v2")
            .propertyMap.as("v2-props")
            .map(x => Direction.OUT).as("direction")
            .back("v2")
        )*/
      }

      val q = if(direction.equals(Direction.OUT)) base.addPipe(out)
              else if(direction.equals(Direction.IN)) base.addPipe(in)
              else both

      query(q,Seq("v1","v1-props","edges","edges-props","v2","v2-props","direction"),tx)
      .map{ results =>
        results.map{row =>
          val v1 = graphBase.rowToRawVertex(row,"v1","v1-props")
          val edge = graphBase.rowToRawEdge(row,"edges","edges-props")
          val v2 = graphBase.rowToRawVertex(row,"v2","v2-props")
          val dir =
            if(! direction.equals(Direction.BOTH)) direction
            else row.getColumn("direction").asInstanceOf[Direction]

          RawSegment(v1,edge,v2,dir)
        }
        .toList()
      }

    }

    /**
     * @inheritdoc
     */
    def select(filter: GremlinScalaPipeline[Vertex,Vertex], tx : Option[TransactionalGraph] = None) : Future[RawVertex] = {
      query(filter,tx).map { result =>
        if(result.size.equals(0)) throw new ObjectNotFoundException(s"could not find object from query")
        else if(result.size > 1) throw new UnexpectedResultsException(s"error retrieving object with query - found ${result.size} results when expecting 1")
        else result.head
      }
    }

    def validate[T : VertexHelper](obj : T, tx : Option[TransactionalGraph] = None) : Future[T] = {
      val base = new GremlinScalaPipeline[Vertex,Vertex]
        .has("id",T.neq,"foo")
      graphBase.vertexQuery(base,tx).flatMap { pipe =>
        implicitly[VertexHelper[T]].validate(obj, pipe) match {
          case Success(x) => Future.successful(x)
          case Failure(e) => Future.failed(e)
        }
      }
    }

    def validateUpdate[T : VertexHelper](vertex : GraphVertex[T], tx : Option[TransactionalGraph] = None) : Future[T] = {
      val base = new GremlinScalaPipeline[Vertex,Vertex]
           .has("id",T.neq,vertex.id)
      graphBase.vertexQuery(base,tx).flatMap { pipe =>
        implicitly[VertexHelper[T]].validate(vertex.obj, pipe) match {
          case Success(x) => Future.successful(x)
          case Failure(e) => Future.failed(e)
        }
      }
    }


  }
}