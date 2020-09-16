package datomicJava.client.api.sync

import java.util.{List => jList, Map => jMap}
import datomic.Util
import datomic.Util.read
import datomic.client.api.protocols.{Client => DatomicClient}
import datomicClojure.{ClojureBridge, Invoke}
import datomicJava.anomaly.AnomalyWrapper

case class Client(
  forPeerServer: Boolean,
  datomicClient: DatomicClient
) extends ClojureBridge with  AnomalyWrapper {


  def administerSystem(options: jMap[_, _]): jMap[_, _] = catchAnomaly {
    Invoke.administerSystem(datomicClient, edn(options))
  }

  def administerSystem(dbName: String): jMap[_, _] = catchAnomaly {
    administerSystem(Util.map(
      read(":db-name"), dbName,
      read(":action"), read(":upgrade-schema"),
    ))
  }


  def connect(dbName: String): Connection = catchAnomaly {
    Connection(Invoke.connect(datomicClient, dbName))
  }


  def createDatabase(dbName: String): Boolean = createDatabase(dbName, 0)
  def createDatabase(
    dbName: String,
    timeout: Int
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
    true
  }


  def deleteDatabase(dbName: String): Boolean = deleteDatabase(dbName, 0)
  def deleteDatabase(
    dbName: String,
    timeout: Int
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


  // If using Peer Server, this will only show the single db that the Peer Server connects to.

  // Convenience overloads
  def listDatabases(): jList[String] = listDatabases(0, 0, 1000)
  def listDatabases(limit: Int): jList[String] = listDatabases(0, 0, limit)
  def listDatabases(
    timeout: Int,
    offset: Int,
    limit: Int,
  ): jList[String] = catchAnomaly {
    Invoke.listDatabase(datomicClient, timeout, offset, limit)
  }
}
