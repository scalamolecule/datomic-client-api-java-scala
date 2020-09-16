package datomicScala.client.api.sync

import java.util.{Collection => jCollection, List => jList, Map => jMap}
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomicClojure.{ClojureBridge, Invoke}
import datomicScala.anomaly.AnomalyWrapper
import scala.jdk.StreamConverters._


object Datomic extends ClojureBridge with AnomalyWrapper {

  require("datomic.client.api")

  // Providing AWSCredentialsProviderChain
  def clientForCloud(
    region: String,
    system: String,
    endpoint: String,
    credsProvider: AWSCredentialsProviderChain,
    proxyPort: Int
  ): Client = Client(
    false,
    Invoke.clientCloudAWS(region, system, endpoint, credsProvider, proxyPort)
  )

  // Providing creds-profile name
  def clientForCloud(
    region: String,
    system: String,
    endpoint: String,
    credsProfile: String,
    proxyPort: Int
  ): Client = Client(
    false,
    Invoke.clientCloudCredsProfile(region, system, endpoint, credsProfile, proxyPort)
  )


  def clientForDevLocal(
    system: String,
    storageDir: String = "" // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): Client = Client(false, Invoke.clientDevLocal(system, storageDir))


  def clientForPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean = false
  ): Client = {
    Client(
      true,
      Invoke.clientPeerServer(accessKey, secret, endpoint, validateHostnames)
    )
  }

  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def q(argMap: jMap[_, _]): jCollection[jList[AnyRef]] = catchAnomaly {
    Invoke.q(argMap)
  }

  // Query as data structure
  def q(query: jList[_], db: Db, args: Any*): jCollection[jList[AnyRef]] = catchAnomaly {
    Invoke.q(edn(query), db.datomicDb, args: _*)
  }

  // Query as String
  def q(query: String, db: Db, args: Any*): jCollection[jList[AnyRef]] = catchAnomaly {
    Invoke.q(query, db.datomicDb, args: _*)
  }


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def qseq(argMap: jMap[_, _]): LazyList[Any] = catchAnomaly {
    Invoke.qseq(argMap).toScala(LazyList)
  }

  // Query as data structure
  def qseq(query: jList[_], db: Db, args: Any*): LazyList[Any] = catchAnomaly {
    Invoke.qseq(edn(query), db.datomicDb, args: _*).toScala(LazyList)
  }

  def qseq(query: String, db: Db, args: Any*): LazyList[Any] = catchAnomaly {
    Invoke.qseq(query, db.datomicDb, args: _*).toScala(LazyList)
  }
}
