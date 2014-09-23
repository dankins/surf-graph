package com.surf.graph.titan

import java.util.UUID

import com.surf.graph.{EdgeHelper, GraphBaseModule, VertexHelper}
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.util.wrappers.batch.{BatchGraph, VertexIDType}

import scala.collection.JavaConversions.mapAsJavaMap


trait BatchGraphModule
{
  this : TitanRawGraph with GraphBaseModule =>

  lazy val batchGraphRaw = new BatchGraph(titanGraph, VertexIDType.STRING, 5000)
  val batchGraph = new BatchGraphApi()

  class BatchGraphApi {
    def create[T : VertexHelper](obj : T) : Vertex = {
      val objType = implicitly[VertexHelper[T]].objectType
      val objClass = obj.getClass.getSimpleName
      val allProps = implicitly[VertexHelper[T]].toMap(obj) ++ Map("type" -> objType, "class"-> objClass)
      val preparedProps = graphBase.prepareProps(allProps)
      batchGraphRaw.addVertex(UUID.randomUUID(),mapAsJavaMap(preparedProps))
    }

    def createSegment[T : EdgeHelper](v1 : Vertex, edge : T, v2 : Vertex) = {
      val label = implicitly[EdgeHelper[T]].label
      val allProps = implicitly[EdgeHelper[T]].toMap(edge)
      val props = graphBase.prepareProps(allProps)

      batchGraphRaw.addEdge(UUID.randomUUID(),v1,v2,label,mapAsJavaMap(props))
    }
  }
}
