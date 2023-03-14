ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "untitled1"
  )
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"
libraryDependencies += "org.json4s" %% "json4s-native" % "4.0.6"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "4.0.6"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.2"




