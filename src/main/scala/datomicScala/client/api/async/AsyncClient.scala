package datomicScala.client.api.async

import java.util.{List => jList, Map => jMap}
import datomic.Util
import datomic.Util._
import datomicClojure._
import datomicScala.AnomalyWrapper


case class AsyncClient(
  forPeerServer: Boolean,
  asyncDatomicClient: AnyRef
) extends AnomalyWrapper with ClojureBridge {

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
  def administerSystem(options: jMap[_, _]): jMap[_, _] = catchAnomaly {
    InvokeAsync.administerSystem(asyncDatomicClient, options)
  }

  def administerSystem(dbName: String): jMap[_, _] = administerSystem(
    Util.map(
      read(":xdb-name"), read(dbName),
      read(":action"), read(":upgrade-schema"),
    )
  )


  def connect(dbName: String): AsyncConnection = {
    AsyncConnection(
      Channel(
        InvokeAsync.connect(asyncDatomicClient, dbName)
      )
    )
  }


  def createDatabase(
    dbName: String,
    timeout: Int = 0
  ): Channel[Boolean] = catchAnomaly {
    if (forPeerServer)
      throw new RuntimeException(ErrorMsg.createDatabase(dbName))
    Channel[Boolean](
      InvokeAsync.createDatabase(asyncDatomicClient, dbName, timeout)
    )
  }


  def deleteDatabase(
    dbName: String,
    timeout: Int = 0
  ): Channel[Boolean] = catchAnomaly {
    if (forPeerServer)
      throw new RuntimeException(ErrorMsg.deleteDatabase(dbName))
    Channel[Boolean](
      InvokeAsync.deleteDatabase(asyncDatomicClient, dbName, timeout)
    )
  }

  // If using Peer Server, this will only show the single db that Peer Server connects to.
  def listDatabases(
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Channel[jList[String]] = catchAnomaly {
    Channel[jList[String]](
      InvokeAsync.listDatabase(asyncDatomicClient, timeout, offset, limit)
    )
  }
}
