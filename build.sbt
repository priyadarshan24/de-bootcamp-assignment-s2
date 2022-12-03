ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "de-bootcamp-assignment-s2"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.3.1" % "provided"
libraryDependencies += "com.github.daddykotex" %% "courier" % "3.2.0"
