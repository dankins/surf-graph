package com.surf.graph.titan

import com.surf.graph._
import com.thinkaurelius.titan.core.TitanFactory
import org.apache.commons.configuration.BaseConfiguration

trait TitanGraphModule extends GraphModule
with GraphAPIModuleImpl
with GraphQueryModuleImpl
with TitanGraphMutationModule
with GraphSystemModuleImpl
with TitanGraphBaseModule
with GraphQueryExecutionContext
with StandardExecutionContext
with TitanRawGraph
{
  val storageBackend : StorageBackends
  val batch = false
  val fastProperty = true // pre-fetch all properties on first vertex property access
  val forceIndex = true // throw an exception if a graph query cannot be answered using an index
  val indexBackend = "elasticsearch"
}

trait TitanCassandraGraphModule extends TitanGraphModule {
  val storageBackend = StorageBackends.cassandra
  val storageHostname : String
  val storagePort = "9160"
  val indexHostname : String
  val indexPort = 9300

  lazy val conf = new BaseConfiguration() {
    setProperty("storage.backend", "cassandra")
    setProperty("storage.hostname", storageHostname)
    setProperty("storage.port", storagePort)
    setProperty("storage.batch-loading", batch)
    setProperty("query.fast-property",fastProperty)
    setProperty("query.force-index",forceIndex)
    setProperty("index.search.backend",indexBackend)
    setProperty("index.search.hostname",indexHostname)
    setProperty("index.search.port",indexPort)
  }
}

trait TitanBerkeleyDBGraphModule  extends TitanGraphModule {
  val storageBackend = StorageBackends.berkeleyje
  val storageDirectory : String
  val indexDirectory : String

  lazy val conf = new BaseConfiguration() {
    setProperty("storage.backend", "berkeleyje")
    setProperty("storage.directory", storageDirectory)
    setProperty("storage.batch-loading", batch)
    setProperty("query.fast-property",fastProperty)
    setProperty("query.force-index",forceIndex)

  }
}

trait TitanInMemoryGraphModule  extends TitanGraphModule {

  val storageBackend = StorageBackends.inmemory
  val indexDirectory = "/tmp/titanindex"
  //val indexConfigFile = getClass.getResource("inmemory_elasticsearch.yml").getFile

  lazy val conf = new BaseConfiguration() {
    setProperty("storage.backend", "inmemory")
    setProperty("storage.batch-loading", batch)
    setProperty("query.fast-property",fastProperty)
    setProperty("query.force-index",forceIndex)
    //setProperty("index.search.backend","elasticsearch")
    //setProperty("index.search.directory",indexDirectory)
    //setProperty("index.search.elasticsearch.local-mode","true")
    //setProperty("index.search.elasticsearch.client-only","false")
  }
}