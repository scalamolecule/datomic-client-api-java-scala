import sbt.Keys._
import sbt.librarymanagement.Resolver.mavenLocal

name := "datomic-client-api-scala"
version := "0.1.0"
scalaVersion := "2.13.3"

organization := "org.scalamolecule"
organizationName := "ScalaMolecule"
organizationHomepage := Some(url("http://www.scalamolecule.org"))

scalacOptions := Seq(
  "-feature",
  "-language:implicitConversions",
  "-language:existentials",
  "-Yrangepos"
)

resolvers ++= Seq(
  ("datomic" at "http://files.datomic.com/maven").withAllowInsecureProtocol(true),
  ("clojars" at "http://clojars.org/repo").withAllowInsecureProtocol(true),
  ("ICM repository" at "http://maven.icm.edu.pl/artifactory/repo/").withAllowInsecureProtocol(true),
  mavenLocal
)
crossPaths := false

libraryDependencies ++= Seq(
  // datomic-free uses 1.8, but we need >=1.9 (otherwise `int?` method is missing)
  "org.clojure" % "clojure" % "1.10.1",

  // Peer / Peer server
  "com.datomic" % "datomic-free" % "0.9.5697",

  // Client
  "com.datomic" % "client-pro" % "0.9.63",

  // Cloud
  "com.datomic" % "client-cloud" % "0.8.102",

  // Dev-local
  "com.datomic" % "dev-local" % "0.9.195",

  "us.bpsm" % "edn-java" % "0.7.1",

  "org.specs2" %% "specs2-core" % "4.10.3" % Test,

  "com.novocode" % "junit-interface" % "0.11" % Test,
  "junit" % "junit" % "4.13" % Test,
  "org.hamcrest" % "hamcrest-junit" % "2.0.0.0" % Test
)
