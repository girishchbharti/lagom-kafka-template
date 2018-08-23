
name := "lagom-kafka-template"

version := "0.1"

scalaVersion := "2.12.6"

import sbt.Keys._
import Dependencies.autoImport._

lazy val `processor` = (project in file("."))
  .aggregate(`processor-api`, `processor-impl`)

lazy val `processor-api` = (project in file("processor-api"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  )

// An external service as kafka producer service
lazy val `external-Service` = (project in file("external-service"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  )

lagomCassandraPort in ThisBuild := 9042
lagomKafkaEnabled in ThisBuild := false
lagomKafkaPort in ThisBuild := 9092

lazy val `processor-impl` = (project in file("processor-impl"))
  .enablePlugins(LagomScala)
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslPersistenceCassandra,
      lagomScaladslKafkaBroker,
      lagomScaladslTestKit,
      macwire
    )
  )
  .settings(lagomForkedTestSettings: _*)
  .dependsOn(`processor-api`)
  .dependsOn(`external-Service`)
