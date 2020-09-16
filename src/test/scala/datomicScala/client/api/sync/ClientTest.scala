package datomicScala.client.api.sync

import datomic.Util
import datomic.Util._
import scala.jdk.CollectionConverters._


class ClientTest extends SetupSpec {
  sequential


  "administer system" in new Setup {
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
  }


  "list databases" in new Setup {
    client.listDatabases().asScala === List("hello")
  }


  "create database" in new Setup {
    if (isDevLocal) {
      client.createDatabase("world")
      client.listDatabases().asScala.sorted === List("hello", "world")
    } else {
      // create-database not implemented for Peer Server
      client.createDatabase("hello") must throwA(
        new RuntimeException(
          """createDatabase is not available with a client running against a Peer Server.
            |Please create a database with the Peer class instead:
            |Peer.createDatabase("datomic:<free/dev/pro>://<host>:<port>/hello")""".stripMargin
        )
      )
    }
  }


  "delete database" in new Setup {
    if (isDevLocal) {
      // Since we run mutable tests in the Scala test suit,
      // the 'world' db created in the test above is still here.
      client.listDatabases().asScala.sorted === List("hello", "world")
      client.deleteDatabase("world")
      client.listDatabases().asScala === List("hello")
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
