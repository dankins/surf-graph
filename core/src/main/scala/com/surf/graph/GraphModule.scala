package com.surf.graph

/**
 * Main entry point for Graphs
 */
trait GraphModule extends GraphAPIModule with IdType
{
  type GraphIdType
  val graph : GraphAPI
  /*
  val objects = new Object with GraphObjects {
    type idType = GraphIdType
  }
  */
}

trait GraphModuleWithLongIds extends GraphModule with LongGraphIds

trait GraphModuleImpl extends GraphModule
  with GraphAPIModuleImpl
  with GraphBaseModuleImpl
  with GraphQueryModuleImpl
  with GraphMutationModuleImpl
  with GraphSystemModuleImpl
  with GraphQueryExecutionContext
  with StandardExecutionContext
{
  this: RawGraph with IdType =>

}