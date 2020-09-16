package datomicScala.client.api.async

import datomic.Util
import datomic.Util._
import datomicScala.AsyncSpec

import scala.jdk.CollectionConverters._


class AsyncClientTest extends AsyncSpec {
  sequential


  "administer system" in new AsyncSetup {
    // todo: Not available for Peer Server?

    client.administerSystem(
      """{
        |:db-name "hello"
        |:action :upgrade-schema
        |}""".stripMargin
    ).toString === "{}"

    client.administerSystem(
      Util.map(
        read(":db-name"), "hello",
        read(":action"), read(":upgrade-schema"),
      )
    ).toString === "{}"

    ok
  }


  "list databases" in new AsyncSetup {
    client.listDatabases().realize.asScala === List("hello")
  }


  "create database" in new AsyncSetup {
    if (isDevLocal) {
      client.createDatabase("world")
      client.listDatabases().realize.asScala.sorted === List("hello", "world")
    } else {
      // create-database not implemented for Peer Server
      client.createDatabase("world") must throwA(
        new RuntimeException(
          """createDatabase is not available with a client running against a Peer Server.
            |Please create a database with the Peer class instead:
            |Peer.createDatabase("datomic:<free/dev/pro>://<host>:<port>/hello")""".stripMargin
        )
      )
    }
  }


  "delete database" in new AsyncSetup {
    if (isDevLocal) {
      // Since we run mutable tests in the Scala test suit,
      // the 'world' db created in the test above is still here.
      client.listDatabases().realize.asScala.sorted === List("hello", "world")
      client.deleteDatabase("world")
      client.listDatabases().realize.asScala === List("hello")
    } else {
      // delete-database not implemented for Peer Server
      client.deleteDatabase("hello") must throwA(
        new RuntimeException(
          """deleteDatabase is not available with a client running against a Peer Server.
            |Please delete a database with the Peer class instead:
            |Peer.deleteDatabase("datomic:<free/dev/pro>://<host>:<port>/hello")""".stripMargin
        )
      )
    }
  }
}
