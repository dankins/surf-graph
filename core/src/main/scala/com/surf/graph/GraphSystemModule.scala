package com.surf.graph

import java.io.InputStream

import com.tinkerpop.blueprints.Graph

trait GraphSystemModule {
  val graphSystem : GraphSystem

  trait GraphSystem {
    def shutdown(implicit graph : Graph) : Unit
    def loadJson(is : InputStream)(implicit graph : Graph) : Unit
  }
}

trait GraphSystemModuleImpl extends GraphSystemModule{
  this : GraphBaseModule =>
  val graphSystem = new GraphSystemImpl

  class GraphSystemImpl extends GraphSystem {
    def shutdown(implicit graph : Graph) = graphBase.shutdown
    def loadJson(is : InputStream)(implicit graph : Graph) = graphBase.loadJson(is)
  }
}
