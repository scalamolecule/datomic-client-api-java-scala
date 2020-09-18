package datomicScala.client.api.sync

import datomic.Util
import datomic.Util._
import datomicClojure.ErrorMsg
import datomicScala.Spec
import scala.jdk.CollectionConverters._


class ClientTest extends Spec {
  sequential


  "administer system" in new Setup {
    // todo: Not available for Peer Server?

    client.administerSystem("hello") === Util.map()

    client.administerSystem(
      Util.map(
        read(":db-name"), "hello",
        read(":action"), read(":upgrade-schema"),
      )
    ) === Util.map()

    // todo - why doesn't this throw a failure exception?
    client.administerSystem("xyz") must throwA(
      new RuntimeException(
        """Some failure message...""".stripMargin
      )
    )
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
      // Since we run mutable tests in the Scala test suit,
      // the 'world' db created in the test above is still here.
      client.listDatabases().asScala.sorted === List("hello", "world")
      client.deleteDatabase("world")
      client.listDatabases().asScala === List("hello")
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
