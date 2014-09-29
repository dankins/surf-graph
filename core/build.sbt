name := "surf-graph-core"

organization := "com.surf.graph"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.2"

resolvers +=
  "Sonatype OSS" at "https://oss.sonatype.org/content/repositories/releases"

libraryDependencies ++= Seq(
  "com.michaelpollmeier" %% "gremlin-scala" % "2.6.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "org.specs2" %% "specs2" % "2.4.2" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test"
)