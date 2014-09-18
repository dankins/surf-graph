package com.surf.graph

import com.tinkerpop.gremlin.scala.{ScalaVertex, GremlinScalaPipeline}
import scala.util.{Failure, Success, Try}
import java.util.UUID
import com.tinkerpop.blueprints.Vertex

trait VertexHelper[T] {
  type GraphPipe = GremlinScalaPipeline[Vertex, Vertex]
  val objectType: String
  val uniqueFields: Seq[String]

  def validate(obj: T, pipe: GraphPipe): Try[T]
  def toObject(props: Map[String, Any]): T
  def toMap(obj: T): Map[String, Any]

  def uniqueVal(obj : T) : Any = {
    // TODO write unit tests to validate removing namespace from field
    val fieldName = uniqueFields.head
    val trimmed = if(fieldName.contains(":")) fieldName.split(":")(1) else fieldName
    val field = obj.getClass.getDeclaredField(trimmed)
    field.setAccessible(true)
    field.get(obj) match {
      case None =>
      case o: Any => o
    }
  }
}

trait DefaultVertexHelper[T] extends VertexHelper[T] {
  val uniqueFields : Seq[String]
  //val fields : Map[String,Any]

  // TODO does this block? should it be a future?
  def validate(obj: T, pipe : GraphPipe): Try[T] = {

    pipe.has("type",objectType)

    def splits(fieldName : String) : GremlinScalaPipeline[Vertex,Any] = {
      // if the unique fieldname has a colon in it, then extract the latter part from the Object
      val trimmed = if(fieldName.contains(":")) fieldName.split(":")(1) else fieldName
      val uniqueField = obj.getClass.getDeclaredField(trimmed)
      uniqueField.setAccessible(true)
      val uniqueVal = uniqueField.get(obj)
      new GremlinScalaPipeline[Vertex,Any]().has(fieldName,uniqueVal)
    }
    if(uniqueFields.size > 0) {
      val searchList = uniqueFields.map(splits)

      val results = pipe.copySplit(searchList : _*).fairMerge.toList()

      if(results.size == 0 ) Success(obj)
      else Failure(new GraphObjectUniqueConstraintException("Unique constraint failed when creating object " + obj.toString))
    } else {
      Success(obj)
    }
  }
}

trait ReadOnlyVertexHelper[T] extends VertexHelper[T] {
  val objectType = "n/a"
  val uniqueFields = Seq()

  def setProperties(v: ScalaVertex, obj : T) = throw new Exception("read only")
  def validate(obj: T, pipe : GraphPipe) = throw new Exception("read only")
  def toMap(obj : T) : Map[String,Any] = {
    throw new Exception("read only")
  }
}

case class SimpleVertex(props : Map[String,Any])
object SimpleVertex {
  implicit object SimpleVertexHelper extends ReadOnlyVertexHelper[SimpleVertex] {
    def toObject(props : Map[String,Any]) : SimpleVertex = {
      SimpleVertex(props)
    }
  }
}


trait EdgeHelper[T] {
  val label : String

  def toMap(obj : T) : Map[String,Any]
  def toObj(props : Map[String,Any]) : T
}
trait SimpleEdgeHelper[T] extends EdgeHelper[T] {
  def toMap(obj : T) : Map[String,Any] = Map()
}

case class SimpleEdge()
object SimpleEdge {
  implicit object SimpleEdgeHelper extends EdgeHelper[SimpleEdge] {
    val label = "SimpleEdge"
    def toMap(obj : SimpleEdge) : Map[String,Any] = Map()
    def toObj(props : Map[String,Any]) = SimpleEdge()
  }
}