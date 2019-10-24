val DefaultScalaVersion = "2.12.9"

val LogServerVersion = "1.2"

import Dependecies._

val commonSettings = Seq(
  version := LogServerVersion,
  scalaVersion := DefaultScalaVersion,

  scalacOptions ++= Seq("-target:jvm-1.8", "-unchecked", "-deprecation", "-feature", "-language:existentials"),
  javacOptions ++= Seq("-source", "1.8"),

  crossPaths := false,

  scalaSource in Compile := baseDirectory.value / "src",
  scalaSource in Test := baseDirectory.value / "test",
  javaSource in Compile := baseDirectory.value / "src",
  javaSource in Test := baseDirectory.value / "test",
  resourceDirectory in Compile := baseDirectory.value / "resource",
  resourceDirectory in Test := baseDirectory.value / "resource"
)

lazy val app = Project(
  id = "log-server",
  base = file("."),
  settings = commonSettings ++ Seq(
    libraryDependencies ++= Seq(
      jacksonDatabind,
      jacksonModuleScala,
      jsr305,
      kolobokeApiJdk8,
      kolobokeImplJdk8,
      logbackClassic,
      specs2Core,
      specs2Mock
    ),

    assemblyJarName in assembly := "log-server.jar",
    mainClass in assembly := Some("Cmd"),

    // Deploy settings
    startYear := Some(2015),
    homepage := Some(url("https://github.com/citrum/log-server")),
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
    bintrayVcsUrl := Some("https://github.com/citrum/log-server"),
    bintrayOrganization := Some("citrum"),
    // No Javadoc
    publishArtifact in(Compile, packageDoc) := false,
    publishArtifact in packageDoc := false,
    sources in(Compile, doc) := Nil,

    // Publish fat jar
    artifact in(Compile, assembly) := {
      val art = (artifact in(Compile, assembly)).value
      art.copy(`classifier` = Some("assembly"))
    }
  )
)

addArtifact(artifact in(Compile, assembly), assembly)
