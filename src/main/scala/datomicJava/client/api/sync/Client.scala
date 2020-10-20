package datomicJava.client.api.sync

import java.util.{List => jList, Map => jMap}
import datomic.Util
import datomic.Util.read
import datomic.client.api.protocols.{Client => DatomicClient}
import datomicClojure.{ClojureBridge, ErrorMsg, Invoke}
import datomicJava.AnomalyWrapper

case class Client(
  forPeerServer: Boolean,
  datomicClient: DatomicClient
) extends ClojureBridge with AnomalyWrapper {

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
    if (forPeerServer)
      throw new RuntimeException(ErrorMsg.administerSystem)
    Invoke.administerSystem(datomicClient, options)
  }

  def administerSystem(dbName: String): jMap[_, _] = administerSystem(
    Util.map(
      read(":db-name"), read(dbName),
      read(":action"), read(":upgrade-schema"),
    )
  )


  def connect(dbName: String): Connection = catchAnomaly {
    Connection(Invoke.connect(datomicClient, dbName))
  }


  def createDatabase(
    dbName: String,
    timeout: Int
  ): Boolean = catchAnomaly {
    if (forPeerServer)
      throw new RuntimeException(ErrorMsg.createDatabase(dbName))
    Invoke.createDatabase(datomicClient, dbName, timeout).asInstanceOf[Boolean]
  }
  def createDatabase(dbName: String): Boolean = createDatabase(dbName, 0)


  def deleteDatabase(
    dbName: String,
    timeout: Int
  ): Boolean = catchAnomaly {
    if (forPeerServer)
      throw new RuntimeException(ErrorMsg.deleteDatabase(dbName))
    Invoke.deleteDatabase(datomicClient, dbName, timeout).asInstanceOf[Boolean]
  }
  def deleteDatabase(dbName: String): Boolean = deleteDatabase(dbName, 0)


  // If using Peer Server, this will only show the single db that the Peer Server connects to.
  def listDatabases(
    timeout: Int,
    offset: Int,
    limit: Int
  ): jList[String] = catchAnomaly {
    Invoke.listDatabase(datomicClient, timeout, offset, limit).asInstanceOf[jList[String]]
  }
  def listDatabases(): jList[String] = listDatabases(0, 0, 1000)
  def listDatabases(limit: Int): jList[String] = listDatabases(0, 0, limit)
}
