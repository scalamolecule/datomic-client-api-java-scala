package datomicScala.client.api.sync

import java.util.{List => jList, Map => jMap}
import datomic.client.api.protocols.{Client => DatomicClient}
import datomicClojure.Invoke
import datomicScala.anomaly.AnomalyWrapper
import datomicScala.util.Helper._

case class Client(
  forPeerServer: Boolean,
  datomicClient: DatomicClient
) extends AnomalyWrapper {

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
    Invoke.administerSystem(datomicClient, edn(options))
  }

  def administerSystem(options: String): jMap[_, _] = catchAnomaly {
    Invoke.administerSystem(datomicClient, options)
  }


  def connect(dbName: String): Connection = catchAnomaly {
    Connection(Invoke.connect(datomicClient, dbName))
  }


  def createDatabase(
    dbName: String,
    timeout: Int = 0
  ): Boolean = catchAnomaly {
    if (forPeerServer) {
      throw new RuntimeException(
        s"""createDatabase is not available with a client running against a Peer Server.
           |Please create a database with the Peer class instead:
           |Peer.createDatabase("datomic:<free/dev/pro>://<host>:<port>/$dbName")""".stripMargin
      )
    } else {
      // Errors reported via ex-info exceptions, with map contents
      // as specified by cognitect.anomalies.
      Invoke.createDatabase(datomicClient, dbName, timeout)
    }
  }


  def deleteDatabase(
    dbName: String,
    timeout: Int = 0
  ): Boolean = catchAnomaly {
    if (forPeerServer) {
      throw new RuntimeException(
        s"""deleteDatabase is not available with a client running against a Peer Server.
           |Please delete a database with the Peer class instead:
           |Peer.deleteDatabase("datomic:<free/dev/pro>://<host>:<port>/$dbName")""".stripMargin
      )
    } else {
      // Errors reported via ex-info exceptions, with map contents
      // as specified by cognitect.anomalies.
      Invoke.deleteDatabase(datomicClient, dbName, timeout)
    }
  }


  // If using Peer Server, this will only show the single db that Peer Server connects to.
  def listDatabases(
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000,
  ): jList[String] = catchAnomaly {
    Invoke.listDatabase(datomicClient, timeout, offset, limit)
  }
}
