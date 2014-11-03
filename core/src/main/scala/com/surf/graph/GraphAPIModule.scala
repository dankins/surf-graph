package com.surf.graph

import java.io.InputStream

import com.tinkerpop.blueprints._
import com.tinkerpop.gremlin.scala.GremlinScalaPipeline
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future

trait GraphAPIModule extends IdType {
  val graph : GraphAPI

  trait GraphAPI {
    import objects._

    /**
     * Takes the passed in object and creates a Vertex
     */
    def create[T: VertexHelper](obj: T) : Future[GraphVertex[T]]

    /**
     * Creates an Edge between two existing Vertices
     */
    def createSegment[V1, E : EdgeHelper, V2](v1 : GraphVertex[V1], edge : E, v2 : GraphVertex[V2], direction : Direction = Direction.OUT) : Future[Segment[V1,E,V2]]

    /**
     * Creates an Edge between two existing Vertices
     */
    def createSegmentUnique[V1, E : EdgeHelper, V2](v1 : GraphVertex[V1], edge : E, v2 : GraphVertex[V2], direction : Direction = Direction.OUT) : Future[Segment[V1,E,V2]]
    /**
     * Retrieves the object with the given vertexId from the Graph
     * It will marshall the object from a property map to an object of type T
     * Wraps the object in a GraphVertex containing graph metadata
     */
    def get[T : VertexHelper](vertexId : idType) : Future[GraphVertex[T]]

    /**
     * TODO
     * @param edgeId - id of the edge to retrieve
     * @tparam T - the type of edge
     * @return
     */
    def getEdge[T : EdgeHelper](edgeId : edgeIdType) : Future[GraphEdge[T]]

    /**
     * Retrieves the object with the given key/value combination
     * Will return an exception if more than one object is retrieved
     */
    def getByKey[T : VertexHelper](key : String, value : Any) : Future[GraphVertex[T]]

    /**
     * Retrieves the Segment with the given edgeId
     * @param edgeId - the ID of the edge to start the query
     * @tparam O - the object type of the Out vertex
     * @tparam E - the object type of the Edge
     * @tparam I - the object type of the In vertex
     * @return - The Segment that contains the two vertices associated with the given Edge
     */
    def getSegment[O : VertexHelper, E : EdgeHelper, I : VertexHelper](edgeId : edgeIdType) : Future[Segment[O,E,I]]


    /**
     * Retrieves the Segment between two vertices with the specified label
     * @param v1 - the ID of the outbound vertex
     * @param v2 - the ID of the inbound vertex
     * @param label - the label of the edge
     * @tparam O - the object type of the Out vertex
     * @tparam E - the object type of the Edge
     * @tparam I - the object type of the In vertex
     * @return - The Segment that contains the two vertices associated with the given Edge
     */
    def getSegmentFromVertices[O : VertexHelper, E : EdgeHelper, I : VertexHelper](v1 : idType, v2 : idType, label : String ) : Future[Option[Segment[O,E,I]]]

    /**
     * Retrieves all vertices linked to the specified vertex
     * @param vertexId - the base vertex ID to start the query from
     * @tparam E - the type of the Edge to retrieve. will use the EdgeHelper implicitly to determine the label to query
     * @tparam V - the type of the Vertices that will be returned in the EdgeTuple
     *@return - returns a list of EdgeTuples containing the edge, direction, and vertex
     */
    def getEdges[E : EdgeHelper,V : VertexHelper](vertexId : idType, direction : Direction) : Future[Seq[EdgeTuple[E,V]]]

    /**
     * Updates the object stored in the graph with the object in the GraphVertex parameter
     * @return will return the updated GraphVertex containing the new object
     */
    def update[T : VertexHelper](v : GraphVertex[T] ) : Future[GraphVertex[T]]

    /**
     * Updates the object stored in the graph with the object in the GraphEdge parameter
     * @return will return the updated GraphEdge containing the new object
     */
    def update[T : EdgeHelper](e : GraphEdge[T] ) : Future[GraphEdge[T]]

    /**
     * Updates the specified property for the given GraphVertex
     * @return will return a string stating success
     */
    def updateProperty[T : VertexHelper](obj : GraphVertex[T], propName : String, newValue : Any ) : Future[GraphVertex[T]]

    /**
     * Updates the specified property for the given GraphEdge
     * @return will return a string stating success
     */
    def updateProperty[T : EdgeHelper](obj : GraphEdge[T], propName : String, newValue : Any ) : Future[GraphEdge[T]]


    /**
     * Removes the vertex and any incident edges for the
     * @return will return a string "operation successful" if successful
     */
    def delete[T : VertexHelper](vertex : GraphVertex[T]) : Future[String]

    /**
     * Removes the Edge between two vertices
     * @return will return a string "operation successful" if successful
     */
    def delete[E : EdgeHelper](edge : GraphEdge[E]) : Future[String]

    /**
     * Checks if the given object is valid and able to be persisted to the graph without error.
     * @return - returns the object back if valid
     */
    def validate[T : VertexHelper](v : T) : Future[T]

    /**
     * Filters using a graph pipeline and returns the matching vertex objects
     * @param pipe - the pipeline used to filter the objects
     * @tparam T - the type of the objects that will be returned
     */

    def queryV[T : VertexHelper](vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]) : Future[Seq[GraphVertex[T]]]
    def queryV[T : VertexHelper](key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]): Future[Seq[GraphVertex[T]]]
    def queryE[E : EdgeHelper](edgeId : edgeIdType)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,Edge]) : Future[Seq[GraphEdge[E]]]
    def queryE[E: EdgeHelper](key : String, value : Any)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,Edge]): Future[Seq[GraphEdge[E]]]

    def genericQueryV[E](vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E]) : Future[Seq[E]]
    def genericQueryV[E](key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E]) : Future[Seq[E]]
    def genericQueryE[E](edgeId : edgeIdType)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E]) : Future[Seq[E]]
    def genericQueryE[E](key : String, value : Any)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E]) : Future[Seq[E]]

    /**
     * Runs the given filter to retrieve a set of Vertices, then retrieves edges specified by the EdgeHelper
     * @param filter - the query to run to retrieve the main set of edges
     * @param direction - the direction of the Edges to retrieve
     * @tparam V1 - type of the in vertex
     * @tparam E - type of the in edge
     * @tparam V2 - type of the out vertex
     */
    def querySegments[V1 : VertexHelper, E : EdgeHelper, V2 : VertexHelper](direction : Direction,vertexId : idType)(filter: GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]) : Future[Seq[Segment[V1,E,V2]]]

    /**
     * TODO
     */
    def querySegments[V1 : VertexHelper, E : EdgeHelper, V2 : VertexHelper](direction : Direction,key : String, value : Any)(filter: GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]) : Future[Seq[Segment[V1,E,V2]]]
    /**
     * Very similar to query, except the end pipeline expects only a single object and will throw an exception otherwise
     * @param pipe - the pipeline used to select the object
     */
    def select[T : VertexHelper](vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]) : Future[GraphVertex[T]]
    def select[T : VertexHelper](key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]) : Future[GraphVertex[T]]

    /**
     * Will close the connection to the graph database
     * @return
     */
    def shutdown() : Unit

    def loadJson(is : InputStream)
  }
}

trait GraphAPIModuleImpl extends GraphAPIModule with StandardExecutionContext {
    this: RawGraph
    with GraphMutationModule
    with GraphQueryModule
    with GraphSystemModule
  =>
  val graph = new GraphApiImpl

  class GraphApiImpl extends GraphAPI with LazyLogging {
    import objects._
    implicit lazy val executionContext = standardExecutionContext

    def txOrRaw(tx : Option[TransactionalGraph]) : Graph = tx.getOrElse(rawGraph)
    /**
     * @inheritdoc
     */
    def create[T: VertexHelper](obj: T) : Future[GraphVertex[T]] = transaction("create")  { tx =>
      val objType = implicitly[VertexHelper[T]].objectType
      val props = implicitly[VertexHelper[T]].toMap(obj)

      logger.debug(s"Creating vertex of type $objType")
      // add the vertex to the graph and return the GraphVertex
      validate(obj,txOrRaw(tx)).flatMap { validated =>
        graphMutation.addVertex(objType, props)(txOrRaw(tx))
          .map(toGraphVertex[T])
      }
    }
    /**
     * @inheritdoc
     */
    def createSegment[V1, E : EdgeHelper, V2](v1 : GraphVertex[V1], edge : E, v2 : GraphVertex[V2], direction : Direction) : Future[Segment[V1,E,V2]] = transaction("createSegment") { tx =>
      val label = implicitly[EdgeHelper[E]].label
      val props = implicitly[EdgeHelper[E]].toMap(edge)

      val (out, in) =
        if(Direction.OUT.equals(direction))
          (v1, v2)
        else (v2, v1)

      graphMutation.addEdge(out.id,in.id,label,props)(txOrRaw(tx)).map { e =>
        Segment[V1,E,V2](v1,toGraphEdge[E](e),v2,direction)
      }
    }

    /**
     * @inheritdoc
     */
    def createSegmentUnique[V1, E : EdgeHelper, V2](v1 : GraphVertex[V1], edge : E, v2 : GraphVertex[V2], direction : Direction) : Future[Segment[V1,E,V2]] = transaction("createSegmentUnique") { tx =>
      val label = implicitly[EdgeHelper[E]].label
      val props = implicitly[EdgeHelper[E]].toMap(edge)

      val (out, in) =
        if(Direction.OUT.equals(direction))
          (v1, v2)
        else (v2, v1)

      graphMutation.addEdgeUnique(out.id,in.id,label,props)(txOrRaw(tx)).map { e =>
        Segment[V1,E,V2](v1,toGraphEdge[E](e),v2,direction)
      }
    }
    /**
     * @inheritdoc
     */
    def get[T : VertexHelper](vertexId : idType) : Future[GraphVertex[T]] = {
      graphQuery.get(vertexId).map { item =>
        toGraphVertex[T](item)
      }
    }

    /**
     * @inheritdoc
     */
    def getEdge[T : EdgeHelper](edgeId : edgeIdType) : Future[GraphEdge[T]] = {
      graphQuery.getEdge(edgeId).map { item =>
        toGraphEdge[T](item)
      }
    }
    /**
     * @inheritdoc
     */
    def getByKey[T : VertexHelper](key : String, value : Any) : Future[GraphVertex[T]] = {
      graphQuery.getByKey(key,value).map(toGraphVertex[T])
    }

    /**
     * @inheritdoc
     */
    def getSegment[O : VertexHelper, E : EdgeHelper, I : VertexHelper](edgeId : edgeIdType) : Future[Segment[O,E,I]] = {
      graphQuery.getSegment(edgeId).map(toGraphSegment[O,E,I])
    }

    /**
     * @inheritdoc
     */
    def getSegmentFromVertices[O : VertexHelper, E : EdgeHelper, I : VertexHelper](v1 : idType, v2 : idType, label : String ) : Future[Option[Segment[O,E,I]]] = {
      graphQuery.getSegmentFromVertices(v1,v2,label).map(_.map(toGraphSegment[O,E,I]))
    }
    /**
     * @inheritdoc
     */
    def getEdges[E : EdgeHelper,V : VertexHelper](vertexId : idType, direction : Direction) : Future[Seq[EdgeTuple[E,V]]] = {
      val label = implicitly[EdgeHelper[E]].label

      graphQuery.getEdges(vertexId,direction,label).map{ rawEdgeTuples =>
        rawEdgeTuples.map(rawEdgeTuple[E,V])
      }
    }
    /**
     * @inheritdoc
     */
    def update[T : VertexHelper](v : GraphVertex[T] ) : Future[GraphVertex[T]] = transaction("update vertex") { tx =>
      val props = implicitly[VertexHelper[T]].toMap(v.obj)
      val allProps = props ++ Map("type" -> v.objType)

      graphQuery.validateUpdate(v)(txOrRaw(tx),implicitly[VertexHelper[T]]).flatMap { x=>
        graphMutation.updateVertex(v.id,allProps)(txOrRaw(tx))
          .map(toGraphVertex[T])
      }
    }

    /**
     * @inheritdoc
     */
    def update[T : EdgeHelper](v : GraphEdge[T] ) : Future[GraphEdge[T]] = transaction("update edge") { tx =>
      val props = implicitly[EdgeHelper[T]].toMap(v.obj)
      graphMutation.updateEdge(v.id,props)(txOrRaw(tx))
        .map(toGraphEdge[T])
    }

    /**
     * @inheritdoc
     */
    def updateProperty[T : VertexHelper](v : GraphVertex[T], propName : String, newValue : Any ) : Future[GraphVertex[T]] = transaction("update vertex property") { tx =>
      graphMutation.updateVertexProperty(v.id,propName,newValue)(txOrRaw(tx))
        .flatMap(x => graphQuery.get(v.id)(txOrRaw(tx)).map(toGraphVertex[T]))
    }

    /**
     * @inheritdoc
     */
    def updateProperty[T : EdgeHelper](e : GraphEdge[T], propName : String, newValue : Any ) : Future[GraphEdge[T]] = transaction("update edge property"){ tx =>
      graphMutation.updateEdgeProperty(e.id,propName,newValue)(txOrRaw(tx))
        .flatMap(x => graphQuery.getEdge(e.id)(txOrRaw(tx)).map(toGraphEdge[T]))
    }

    /**
     * @inheritdoc
     */
    def delete[T : VertexHelper](vertex : GraphVertex[T]) : Future[String] = transaction("delete vertex") { tx =>
      graphMutation.deleteVertex(vertex.id)(txOrRaw(tx))
    }

    /**
     * @inheritdoc
     */
    def delete[E : EdgeHelper](edge : GraphEdge[E]) : Future[String] = transaction("delete edge") { tx =>
      graphMutation.deleteEdge(edge.id)(txOrRaw(tx))
    }

    /**
     * @inheritdoc
     */
    def validate[T : VertexHelper](v : T) : Future[T] = transaction("validate"){ tx =>
      validate(v,txOrRaw(tx))
    }
    private def validate[T : VertexHelper](v : T, graph : Graph) : Future[T] = {
      graphQuery.validate(v)(graph,implicitly[VertexHelper[T]])
    }

    def queryV[T : VertexHelper](vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]) : Future[Seq[GraphVertex[T]]] = {
      graphQuery.queryV(vertexId)(pipe).map(_.map(toGraphVertex[T]))
    }
    def queryV[T : VertexHelper](key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]): Future[Seq[GraphVertex[T]]] = {
      graphQuery.queryV(key,value)(pipe).map(_.map(toGraphVertex[T]))
    }
    def queryE[E : EdgeHelper](edgeId : edgeIdType)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,Edge]) : Future[Seq[GraphEdge[E]]] = {
      graphQuery.queryE(edgeId)(pipe).map(_.map(toGraphEdge[E]))
    }
    def queryE[E: EdgeHelper](key : String, value : Any)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,Edge]): Future[Seq[GraphEdge[E]]] = {
      graphQuery.queryE(key,value)(pipe).map(_.map(toGraphEdge[E]))
    }

    def genericQueryV[E](vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E]) : Future[Seq[E]] = graphQuery.genericQueryV(vertexId)(pipe)
    def genericQueryV[E](key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,E]) : Future[Seq[E]] = graphQuery.genericQueryV(key,value)(pipe)
    def genericQueryE[E](edgeId : edgeIdType)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E]) : Future[Seq[E]] = graphQuery.genericQueryE(edgeId)(pipe)
    def genericQueryE[E](key : String, value : Any)(pipe : GremlinScalaPipeline[Edge,Edge] => GremlinScalaPipeline[Edge,E]) : Future[Seq[E]] = graphQuery.genericQueryE(key,value)(pipe)

    /**
     * Very similar to query, except the end pipeline expects only a single object and will throw an exception otherwise
     * @param pipe - the pipeline used to select the object
     */
    def select[T : VertexHelper](vertexId : idType)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]) : Future[GraphVertex[T]] = {
      graphQuery.select(vertexId)(pipe).map(toGraphVertex[T])
    }
    def select[T : VertexHelper](key : String, value : Any)(pipe : GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]) : Future[GraphVertex[T]] = {
      graphQuery.select(key,value)(pipe).map(toGraphVertex[T])
    }

    /**
     * @inheritdoc
     */
    def querySegments[V1 : VertexHelper, E : EdgeHelper, V2 : VertexHelper](direction : Direction, id : idType)(filter: GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]) : Future[Seq[Segment[V1,E,V2]]] = {
      val edgeLabel = implicitly[EdgeHelper[E]].label

      graphQuery.querySegments(direction,edgeLabel,id)(filter).map { results =>
        results.map { raw =>
          toGraphSegment[V1,E,V2](raw)
        }
      }
    }

    /**
     * @inheritdoc
     */
    def querySegments[V1 : VertexHelper, E : EdgeHelper, V2 : VertexHelper](direction : Direction,key : String, value : Any)(filter: GremlinScalaPipeline[Vertex,Vertex] => GremlinScalaPipeline[Vertex,Vertex]) : Future[Seq[Segment[V1,E,V2]]] = {
      val edgeLabel = implicitly[EdgeHelper[E]].label

      graphQuery.querySegments(direction,edgeLabel,key, value)(filter).map { results =>
        results.map { raw =>
          toGraphSegment[V1,E,V2](raw)
        }
      }
    }
    /**
     * @inheritdoc
     */
    /*
    def select[T : VertexHelper](filter: GremlinScalaPipeline[Vertex,Vertex]) : Future[GraphVertex[T]] = {
      query(filter).map( results =>
        if(results.size == 1) results.head
        else if (results.size == 0) throw new ObjectNotFoundException("could not find object from query")
        else throw new UnexpectedResultsException("expected 1 result, but found "+results.size)
      )
    }
    */

    /**
     * @inheritdoc
     */
    def shutdown() : Unit = graphSystem.shutdown

    def loadJson(is : InputStream) = {
      graphSystem.loadJson(is)
    }
  }

}
