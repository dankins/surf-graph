name := "surf-graph-titan"

organization := "com.surf.graph"

version := "0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "2.4.2" % "test",
  "com.surf.graph" % "surf-graph-core_2.10" % "0.1-SNAPSHOT",
  "com.michaelpollmeier" % "gremlin-scala" % "2.4.2",
  "com.thinkaurelius.titan" % "titan-cassandra" % "0.5.0",
  "com.thinkaurelius.titan" % "titan-berkeleyje" % "0.5.0",
  "com.thinkaurelius.titan" % "titan-core" % "0.5.0"
)