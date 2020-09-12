package datomicJavaScala.client.api.async

import java.util.{List => jList, Map => jMap}
import datomic.Util._
import datomicJavaScala.util.ClojureBridge


case class AsyncClient(
  forPeerServer: Boolean,
  asyncDatomicClient: AnyRef
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
    datomicAsyncFn("administer-system")
      .invoke(asyncDatomicClient, read(edn(options)))
      .asInstanceOf[jMap[_, _]]
  }

  def administerSystem(options: String): jMap[_, _] = {
    datomicAsyncFn("administer-system")
      .invoke(asyncDatomicClient, read(options))
      .asInstanceOf[jMap[_, _]]
  }


  def connect(dbName: String): AsyncConnection = {
    AsyncConnection(
      new Channel(
        datomicAsyncFn("connect")
          .invoke(asyncDatomicClient, read(s"""{:db-name "$dbName"}"""))
      )
    )
  }


  def createDatabase(dbName: String): Channel[Boolean] = {
    if (forPeerServer)
      throw new RuntimeException(
        """createDatabase is not available with a client running against a Peer Server.
          |Please create a database with the Peer class instead:
          |Peer.createDatabase("datomic:free://localhost:4334/hello")""".stripMargin
      )
    new Channel[Boolean](
      datomicAsyncFn("create-database")
        .invoke(asyncDatomicClient, read(s"""{:db-name "$dbName"}"""))
    )
  }


  def deleteDatabase(dbName: String): Channel[Boolean] = {
    if (forPeerServer)
      throw new RuntimeException(
        """deleteDatabase is not available with a client running against a Peer Server.
          |Please delete a database with the Peer class instead:
          |Peer.deleteDatabase("datomic:free://localhost:4334/hello")""".stripMargin)

    new Channel[Boolean](
      datomicAsyncFn("delete-database")
        .invoke(asyncDatomicClient, read(s"""{:db-name "$dbName"}"""))
    )
  }

  // If using Peer Server, this will only show the single db that Peer Server connects to.
  def listDatabases(
    timeoutOpt: Option[Int] = None,
    offsetOpt: Option[Int] = None,
    limit: Int = 1000,
  ): Channel[jList[String]] = {
    val timeout = timeoutOpt.map(t => s":timeout $t ")
    val offset  = offsetOpt.map(t => s":offset $t ")
    new Channel[jList[String]](
      datomicAsyncFn("list-databases").invoke(
        asyncDatomicClient,
        read(
          s"""{
             |$timeout
             |$offset
             |:limit "$limit"
             |}""".stripMargin
        )
      )
    )
  }
}
