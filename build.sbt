import sbt.Keys._
import sbt.librarymanagement.Resolver.mavenLocal
import sbt.url

name := "datomic-client-api-java-scala"
version := "0.2.0"
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
  // Please download from https://cognitect.com/dev-tools and install locally per included instructions
  "com.datomic" % "dev-local" % "0.9.203",

  "us.bpsm" % "edn-java" % "0.7.1",

  "co.fs2" %% "fs2-core" % "2.4.4",

  "org.specs2" %% "specs2-core" % "4.10.3" % Test,

  "com.novocode" % "junit-interface" % "0.11" % Test,
  "junit" % "junit" % "4.13" % Test,
  "org.hamcrest" % "hamcrest-junit" % "2.0.0.0" % Test
)

publishMavenStyle := true
publishTo := (if (isSnapshot.value)
  Some("Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/")
else
  Some("Sonatype OSS Staging" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/"))
publishArtifact in Test := false
scalacOptions in Compile in doc ++= Seq(
  "-doc-root-content", baseDirectory.value + "/src/main/scaladoc/rootdoc.txt",
  "-diagrams", "-groups",
  "-doc-version", version.value,
  "-doc-title", "Molecule",
  "-sourcepath", (baseDirectory in ThisBuild).value.toString,
  "-doc-source-url", s"https://github.com/scalamolecule/molecule/tree/master€{FILE_PATH}.scala#L1"
)
pomIncludeRepository := (_ => false)
homepage := Some(url("http://scalamolecule.org"))
description := "datomic-client-api-java-scala"
licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
scmInfo := Some(ScmInfo(
  url("https://github.com/scalamolecule/datomic-client-api-java-scala"),
  "scm:git:git@github.com:scalamolecule/datomic-client-api-java-scala.git"
))
developers := List(
  Developer(
    id = "marcgrue",
    name = "Marc Grue",
    email = "marcgrue@gmail.com",
    url = url("http://marcgrue.com")
  )
)
