import sbt._

object Dependecies {

  private object Version {
    val jackson = "2.10.0"
    val jsr305 = "3.0.2"
    val koloboke = "1.0.0"
    val logback = "1.2.3"
    val specs2 = "4.8.0"
  }

  // JSON support
  val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % Version.jackson
  val jacksonModuleScala = "com.fasterxml.jackson.module" %% "jackson-module-scala" % Version.jackson exclude("com.google.guava", "guava")

  // Nullable annotations
  val jsr305 = "com.google.code.findbugs" % "jsr305" % Version.jsr305

  // Fast Map collections
  val kolobokeApiJdk8 = "com.koloboke" % "koloboke-api-jdk8" % Version.koloboke
  val kolobokeImplJdk8 = "com.koloboke" % "koloboke-impl-jdk8" % Version.koloboke

  // Logging
  val logbackClassic = "ch.qos.logback" % "logback-classic" % Version.logback

  // Testing
  val specs2Core = "org.specs2" %% "specs2-core" % Version.specs2 % Test
  val specs2Mock = "org.specs2" %% "specs2-mock" % Version.specs2 % Test

}
