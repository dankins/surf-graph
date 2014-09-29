name := "surf-graph-json"

organization := "com.surf.graph"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % "2.3.4",
  "com.surf.graph" % "surf-graph-core_2.10" % "0.1-SNAPSHOT"
)