name := "surf-graph-core"

organization := "com.surf.graph"

version := "0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.michaelpollmeier" % "gremlin-scala" % "2.4.2",
  "org.specs2" %% "specs2" % "2.4.2" % "test"
  // Add your own project dependencies in the form:
  // "group" % "artifact" % "version"
)