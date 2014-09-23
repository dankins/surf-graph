package com.surf.graph.titan

import com.surf.graph._
import org.apache.commons.configuration.BaseConfiguration

trait TitanGraphModule extends GraphModule
with GraphAPIModuleImpl
with GraphQueryModuleImpl
with GraphMutationModuleImpl
with GraphSystemModuleImpl
with TitanGraphBaseModule
with GraphQueryExecutionContext
with StandardExecutionContext
with TitanRawGraph
{
  val storageBackend : StorageBackends
}

trait TitanCassandraGraphModule extends TitanGraphModule {
  val storageBackend = StorageBackends.cassandra
  val storageHostname : String
  val storagePort = "9160"

  lazy val conf = new BaseConfiguration() {
    setProperty("storage.backend", "cassandra")
    setProperty("storage.hostname", storageHostname)
    setProperty("storage.port", storagePort)
    setProperty("storage.batch-loading", batch)
  }
}

trait TitanBerkeleyDBGraphModule  extends TitanGraphModule {
  val storageBackend = StorageBackends.berkeleyje
  val storageLocation : String

  lazy val conf = new BaseConfiguration() {
    setProperty("storage.backend", "berkeleyje")
    setProperty("storage.directory", storageLocation)
    setProperty("storage.batch-loading", batch)
  }
}

trait TitanInMemoryGraphModule  extends TitanGraphModule {
  val storageBackend = StorageBackends.inmemory

  lazy val conf = new BaseConfiguration() {
    setProperty("storage.backend", "inmemory")
    setProperty("storage.batch-loading", batch)
  }
}