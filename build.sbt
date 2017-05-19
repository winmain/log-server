val DefaultScalaVersion = "2.11.11"

lazy val app = Project(id = "log-server", base = file("."), settings = Seq(
  version := "1.0",

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

  assemblyJarName in assembly := "log-server.jar",
  mainClass in assembly := Some("Cmd"),

  libraryDependencies += "org.specs2" %% "specs2-core" % "3.7" % "test",
  libraryDependencies += "org.specs2" %% "specs2-mock" % "3.7" % "test",
  libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2", // Logging
  libraryDependencies += "net.openhft" % "koloboke-api-jdk8" % "0.6.8", // Fast Map collections
  libraryDependencies += "net.openhft" % "koloboke-impl-jdk8" % "0.6.8",
  libraryDependencies += "com.google.code.findbugs" % "jsr305" % "2.0.1",  // Nullable annotations
  libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5", // JSON support
  libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.3" exclude("com.google.guava", "guava") // JSON support
))
