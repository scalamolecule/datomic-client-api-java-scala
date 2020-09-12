package datomicJavaScala.client.api.sync

import java.util.{List => jList, Map => jMap}
import datomic.client.api.protocols.{Client => DatomicClient}
import datomicJavaScala.util.ClojureBridge
import datomic.Util._


case class Client(
  forPeerServer: Boolean,
  datomicClient: DatomicClient
) extends ClojureBridge {

  /**
   * Upgrading Datomic Schema
   *
   * (more system operations might be added)
   *
   * @see https://docs.datomic.com/on-prem/deployment.html#upgrading-schema
   * @see https://docs.datomic.com/cloud/operation/howto.html#upgrade-base-schema
   * @since 0.9.5893
   * @param options param-value pairs of config
   *                // Example options
   *                Util.map(
   *                // Client
   *                read(":db-name"), "my-db",
   *                // On-Prem (?)
   *                read(":uri"), "datomic:dev://localhost:4334/my-db",
   *
   *                read(":action"), read(":upgrade-schema"),
   *                )
   * @return Diagnostive value or throwing a failure exception
   */
  def administerSystem(options: jMap[_, _]): jMap[_, _] = {
    syncFn("administer-system")
      .invoke(datomicClient, read(edn(options)))
      .asInstanceOf[jMap[_, _]]
  }

  def administerSystem(options: String): jMap[_, _] = {
    syncFn("administer-system")
      .invoke(datomicClient, read(options))
      .asInstanceOf[jMap[_, _]]
  }


  def connect(dbName: String): Connection = Connection(
    syncFn("connect").invoke(datomicClient, read(s"""{:db-name "$dbName"}"""))
  )


  def createDatabase(dbName: String): Boolean = {
    if (forPeerServer) {
      throw new RuntimeException(
        """createDatabase is not available with a client running against a Peer Server.
          |Please create a database with the Peer class instead:
          |Peer.createDatabase("datomic:free://localhost:4334/hello")""".stripMargin
      )
    } else {
      // Errors reported via ex-info exceptions, with map contents
      // as specified by cognitect.anomalies.
      syncFn("create-database").invoke(datomicClient, read(s"""{:db-name "$dbName"}"""))
    }
    true
  }


  def deleteDatabase(dbName: String): Boolean = {
    if (forPeerServer) {
      throw new RuntimeException(
        """deleteDatabase is not available with a client running against a Peer Server.
          |Please delete a database with the Peer class instead:
          |Peer.deleteDatabase("datomic:free://localhost:4334/hello")""".stripMargin
      )
    } else {
      // Errors reported via ex-info exceptions, with map contents
      // as specified by cognitect.anomalies.
      syncFn("delete-database").invoke(datomicClient, read(s"""{:db-name "$dbName"}"""))
    }
    true
  }


  // If using Peer Server, this will only show the single db that Peer Server connects to.
  def listDatabases(
    timeoutOpt: Option[Int] = None,
    offsetOpt: Option[Int] = None,
    limit: Int = 1000,
  ): jList[String] = {
    val timeout = timeoutOpt.map(t => s":timeout $t ")
    val offset  = offsetOpt.map(t => s":offset $t ")
    syncFn("list-databases").invoke(
      datomicClient,
      read(
        s"""{
           |$timeout
           |$offset
           |:limit "$limit"
           |}""".stripMargin
      )
    ).asInstanceOf[jList[String]]
  }
}
