package datomicScala.client.api.sync

import datomic.Util
import datomic.Util._
import datomicClient.ErrorMsg
import datomicScala.Spec
import scala.jdk.CollectionConverters._


class ClientTest extends Spec {

  "administer system" in new Setup {
    if (system == "peer-server") {
      // administer-system not implemented for Peer Server
      client.administerSystem("hello") must throwA(
        new RuntimeException(ErrorMsg.administerSystem)
      )
    } else {
      // db name
      client.administerSystem("hello") === Util.map()

      // args map
      client.administerSystem(
        Util.map(
          read(":db-name"), "hello",
          read(":action"), read(":upgrade-schema"),
        )
      ) === Util.map()

      // todo - why doesn't this throw a failure exception with dev-local?
      client.administerSystem("xyz") must throwA(
        new RuntimeException(
          """Some failure message...""".stripMargin
        )
      )
    }
  }

  // (`connect` is tested in ClientTest...)

  "create database" in new Setup {
    if (isDevLocal) {
      client.createDatabase("world")
      client.listDatabases().asScala.sorted === List("hello", "world")
    } else {
      // create-database not implemented for Peer Server
      client.createDatabase("hello") must throwA(
        new RuntimeException(ErrorMsg.createDatabase("hello"))
      )
    }
  }


  "delete database" in new Setup {
    if (isDevLocal) {
      client.deleteDatabase("hello")
      client.listDatabases().asScala === List()
    } else {
      // delete-database not implemented for Peer Server
      client.deleteDatabase("hello") must throwA(
        new RuntimeException(ErrorMsg.deleteDatabase("hello"))
      )
    }
  }


  "list databases" in new Setup {
    client.listDatabases().asScala === List("hello")
  }
}
