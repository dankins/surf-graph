package com.surf.graph

/**
 * Created by dan on 7/8/14.
 */
trait GraphModule
  extends GraphAPIModule
{
  val graph : GraphAPI
}

trait GraphModuleImpl extends GraphModule
  with GraphAPIModuleImpl
    with GraphQueryModuleImpl
    with GraphMutationModuleImpl
    with GraphMarshallingModuleImpl
    with GraphSystemModuleImpl
    with GraphBaseModuleImpl
    with GraphQueryExecutionContext
    with StandardExecutionContext
{
  this : GraphConfigModule =>

  val graph = new GraphApiImpl
}