ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "flight-data"
  )

val sparkVersion = "3.2.1"
libraryDependencies ++= Seq( "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalactic" %% "scalactic" % "3.2.11",
  "org.scalatest" %% "scalatest" % "3.2.11" % "test"
//  "org.apache.spark" % "spark-tags_2.11" % "2.0.0-preview"
//  "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
)
//"org.scalatest" %% "scalatest" % "3.0.8" % Test