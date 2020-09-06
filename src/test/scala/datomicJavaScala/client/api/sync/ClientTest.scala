package datomicJavaScala.client.api.sync

import datomic.Util
import datomicJavaScala.{Setup, SetupSpec}
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
        Util.read(":db-name"), "hello",
        Util.read(":action"), Util.read(":upgrade-schema"),
      )
    ).toString === "{}"

    ok
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
      client.createDatabase("world") must throwA(
        new RuntimeException(
          """createDatabase is not available with a client running against a Peer Server.
            |Please create a database with the Peer class instead:
            |Peer.createDatabase("datomic:free://localhost:4334/hello")""".stripMargin
        )
      )
    }
  }


  "delete database" in new Setup {
    if (isDevLocal) {
      client.listDatabases().asScala.sorted === List("hello", "world")
      client.deleteDatabase("world")
      client.listDatabases().asScala === List("hello")
    } else {
      // delete-database not implemented for Peer Server
      client.deleteDatabase("world") must throwA(
        new RuntimeException(
          """deleteDatabase is not available with a client running against a Peer Server.
            |Please delete a database with the Peer class instead:
            |Peer.deleteDatabase("datomic:free://localhost:4334/hello")""".stripMargin
        )
      )
    }
  }
}
