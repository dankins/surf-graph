name := "rexster"

organization := "com.surf.graph"

version := "0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "2.4.2" % "test",
  "com.surf.graph" % "surf-graph-core_2.10" % "0.1-SNAPSHOT",
  "com.michaelpollmeier" % "gremlin-scala" % "2.4.2",
  "com.tinkerpop.rexster" % "rexster-protocol" % "2.4.0" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  ),
  "com.tinkerpop.blueprints" % "blueprints-rexster-graph" % "2.4.0"
)