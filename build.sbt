val LogServerVersion = "1.2.1"

val DefaultScalaVersion = "2.12.10"

import Dependecies._

val commonSettings = Seq(
  version := LogServerVersion,
  crossScalaVersions := Seq(DefaultScalaVersion, "2.13.2"),
  scalaVersion := DefaultScalaVersion,
  organization := "com.github.winmain",

  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-unchecked",
    "-deprecation",
    "-feature",
    "-language:existentials",
    "-language:implicitConversions"
  ),
  javacOptions ++= Seq("-source", "1.8"),

  scalaSource in Compile := baseDirectory.value / "src",
  scalaSource in Test := baseDirectory.value / "test",
  javaSource in Compile := baseDirectory.value / "src",
  javaSource in Test := baseDirectory.value / "test",
  resourceDirectory in Compile := baseDirectory.value / "resource",
  resourceDirectory in Test := baseDirectory.value / "resource",

  // Deploy settings
  startYear := Some(2015),
  homepage := Some(url("https://github.com/citrum/log-server")),
  licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
  bintrayVcsUrl := Some("https://github.com/citrum/log-server"),
  bintrayOrganization := Some("citrum"),

  // No Javadoc
  publishArtifact in(Compile, packageDoc) := false,
  publishArtifact in packageDoc := false,
  sources in(Compile, doc) := Nil
)

lazy val core = Project(
  id = "log-server-core",
  base = file("./log-server-core"),
  settings = commonSettings ++ Seq(
    libraryDependencies ++= Seq(jacksonAnnotations)
  )
).disablePlugins(sbtassembly.AssemblyPlugin)

lazy val client = Project(
  id = "log-server-client",
  base = file("./log-server-client"),
  settings = commonSettings ++ Seq(
    libraryDependencies ++= Seq(slf4jApi)
  )
).disablePlugins(sbtassembly.AssemblyPlugin).dependsOn(core)

lazy val db = Project(
  id = "log-server-db",
  base = file("./log-server-db"),
  settings = commonSettings ++ Seq(
    libraryDependencies ++= Seq(
      jacksonDatabind,
      jacksonModuleScala,
      jimfs,
      jsr305,
      kolobokeApiJdk8,
      kolobokeImplJdk8,
      slf4jApi,
      specs2Core,
      specs2Mock
    )
  )
).disablePlugins(sbtassembly.AssemblyPlugin).dependsOn(core)

lazy val app = Project(
  id = "log-server",
  base = file("."),
  settings = commonSettings ++ Seq(
    libraryDependencies ++= Seq(
      jimfs,
      logbackClassic,
      slf4jNop,
      specs2Core,
      specs2Mock
    ),

    crossPaths := false,

    assemblyJarName in assembly := "log-server.jar",
    mainClass in assembly := Some("com.github.winmain.logserver.Cmd"),
    assemblyMergeStrategy in assembly := {
      case "module-info.class" =>
        MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },

    // Publish fat jar
    artifact in(Compile, assembly) := {
      val art = (artifact in(Compile, assembly)).value
      art.copy(`classifier` = Some("assembly"))
    }
  )
).dependsOn(client, db).aggregate(core, client, db)

addArtifact(artifact in(Compile, assembly), assembly)
