name := "surf-graph-titan"

organization := "com.surf.graph"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "2.4.2" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test",
  "com.surf.graph" %% "surf-graph-core" % "0.1-SNAPSHOT",
  "com.thinkaurelius.titan" % "titan-core" % "0.5.0",
  "com.thinkaurelius.titan" % "titan-cassandra" % "0.5.0",
  "com.thinkaurelius.titan" % "titan-berkeleyje" % "0.5.0",
  "com.thinkaurelius.titan" % "titan-es" % "0.5.0"
)