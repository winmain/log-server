val DefaultScalaVersion = "2.11.11"

lazy val app = Project(id = "log-server", base = file("."), settings = Seq(
  version := "1.1",

  scalaVersion := DefaultScalaVersion,
  scalacOptions ++= Seq("-target:jvm-1.8", "-unchecked", "-deprecation", "-feature", "-language:existentials"),
  javacOptions ++= Seq("-source", "1.8"),
  crossPaths := false,
  scalaSource in Compile := baseDirectory.value / "src",
  scalaSource in Test := baseDirectory.value / "test",
  javaSource in Compile := baseDirectory.value / "src",
  javaSource in Test := baseDirectory.value / "test",
  resourceDirectory in Compile := baseDirectory.value / "resource",
  resourceDirectory in Test := baseDirectory.value / "resource",

  libraryDependencies += "org.specs2" %% "specs2-core" % "3.7" % "test",
  libraryDependencies += "org.specs2" %% "specs2-mock" % "3.7" % "test",
  libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2", // Logging
  libraryDependencies += "net.openhft" % "koloboke-api-jdk8" % "0.6.8", // Fast Map collections
  libraryDependencies += "net.openhft" % "koloboke-impl-jdk8" % "0.6.8",
  libraryDependencies += "com.google.code.findbugs" % "jsr305" % "2.0.1", // Nullable annotations
  libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5", // JSON support
  libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.3" exclude("com.google.guava", "guava"), // JSON support

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
))

addArtifact(artifact in(Compile, assembly), assembly)
