name := "surf-graph-rexster"

organization := "com.surf.graph"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "2.4.2" % "test",
  "com.surf.graph" %% "surf-graph-core" % "0.1-SNAPSHOT",
  "com.tinkerpop.rexster" % "rexster-protocol" % "2.6.0" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  ),
  "com.tinkerpop.blueprints" % "blueprints-rexster-graph" % "2.6.0"
)