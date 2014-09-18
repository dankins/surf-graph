package com.surf.graph

import java.io.InputStream

/**
 * Created by dan on 7/9/14.
 */
trait GraphSystemModule {
  val graphSystem : GraphSystem

  trait GraphSystem {
    def shutdown() : Unit
    def loadJson(is : InputStream)
  }
}

trait GraphSystemModuleImpl extends GraphSystemModule{
  this : GraphBaseModule =>
  val graphSystem = new GraphSystemImpl

  class GraphSystemImpl extends GraphSystem {
    def shutdown() = graphBase.shutdown()
    def loadJson(is : InputStream) = graphBase.loadJson(is)
  }
}
