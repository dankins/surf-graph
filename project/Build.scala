import sbt._
import Keys._

object SurfGraphBuild extends Build {
  lazy val root = Project(id = "surf-graph",
    base = file(".")) aggregate(core,titan, tinkerpop)

  lazy val core = Project(id = "surf-graph-core",
    base = file("core"))

  lazy val json = Project(id = "surf-graph-json",
    base = file("json"))

  lazy val rexster = Project(id = "surf-graph-rexster",
    base = file("rexster")) dependsOn core

  lazy val tinkerpop = Project(id = "surf-graph-tinkerpop",
    base = file("tinkerpop")) dependsOn core

  lazy val titan = Project(id = "surf-graph-titan",
    base = file("titan")) dependsOn core


}