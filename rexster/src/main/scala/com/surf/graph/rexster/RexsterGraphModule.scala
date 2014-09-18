package com.surf.graph.rexster

import com.surf.graph._
import com.surf.graph.rexster.RexsterGraphConfigModule

trait RexsterGraphModule extends GraphModule
with GraphAPIModuleImpl
with GraphQueryModuleImpl
with GraphMutationModuleImpl
with GraphMarshallingModuleImpl
with GraphSystemModuleImpl
with GraphBaseModuleImpl
with GraphQueryExecutionContext
with StandardExecutionContext
with RexsterGraphConfigModule
{
  val graph = new GraphApiImpl
}