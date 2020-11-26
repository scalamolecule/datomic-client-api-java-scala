package datomicScala.client.api.async

import datomic.Util
import datomic.Util._
import datomicClient.ErrorMsg
import datomicScala.SpecAsync
import scala.jdk.CollectionConverters._


class AsyncClientTest extends SpecAsync {

  // (same as sync version)
  "administer system" in new AsyncSetup {
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

  // (`connect` is tested in AsyncClientTest...)

  "create database" in new AsyncSetup {
    if (isDevLocal) {
      waitFor(client.createDatabase("world"))
      waitFor(client.listDatabases()).toOption.get.asScala.sorted === List("hello", "world")

    } else {
      // create-database not implemented for Peer Server
      client.createDatabase("world") must throwA(
        new RuntimeException(ErrorMsg.createDatabase("hello"))
      )
    }
  }


  "delete database" in new AsyncSetup {
    if (isDevLocal) {
      waitFor(client.deleteDatabase("hello"))
      waitFor(client.listDatabases()).toOption.get.asScala === List()
    } else {
      // delete-database not implemented for Peer Server
      client.deleteDatabase("hello") must throwA(
        new RuntimeException(ErrorMsg.deleteDatabase("hello"))
      )
    }
  }


  "list databases" in new AsyncSetup {
    waitFor(client.listDatabases()).toOption.get.asScala === List("hello")
  }
}
