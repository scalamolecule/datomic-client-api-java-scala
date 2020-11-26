package datomicJava.client.api.sync

import java.util.stream.{Stream => jStream}
import java.util.{Collection => jCollection, List => jList, Map => jMap}
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomic.Util
import datomic.Util.{list, read}
import datomicClient._
import datomicClient.anomaly.AnomalyWrapper
import scala.annotation.varargs

object Datomic extends ClojureBridge with AnomalyWrapper {

  require("datomic.client.api")

  // Providing AWSCredentialsProviderChain
  def clientCloud(
    region: String,
    system: String,
    endpoint: String,
    credsProvider: AWSCredentialsProviderChain,
    proxyPort: Int
  ): Client = Client(
    false,
    Invoke.clientCloudAWS(
      region, system, endpoint, credsProvider, proxyPort
    )
  )

  // Providing creds-profile name
  def clientCloud(
    region: String,
    system: String,
    endpoint: String,
    credsProfile: String,
    proxyPort: Int
  ): Client = Client(
    false,
    Invoke.clientCloudCredsProfile(
      region, system, endpoint, credsProfile, proxyPort
    )
  )


  def clientDevLocal(
    system: String,
    storageDir: String // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): Client = Client(
    false,
    Invoke.clientDevLocal(system, storageDir)
  )

  def clientDevLocal(
    system: String
  ): Client = clientDevLocal(system, "")


  def clientPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean
  ): Client = Client(
    true,
    Invoke.clientPeerServer(accessKey, secret, endpoint, validateHostnames)
  )

  def clientPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
  ): Client = clientPeerServer(accessKey, secret, endpoint, false)


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def q(argMap: jMap[_, _]): jCollection[jList[AnyRef]] = {
    Invoke.q(argMap).asInstanceOf[jCollection[jList[AnyRef]]]
  }

  // Query as data structure
  @varargs
  def q(query: jList[_], db: Db, args: Any*): jCollection[jList[AnyRef]] = {
    q(Util.map(
      read(":query"), edn(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }

  // Query as String
  @varargs
  def q(query: String, db: Db, args: Any*): jCollection[jList[AnyRef]] = {
    q(Util.map(
      read(":query"), read(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }

  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def qseq(map: jMap[_, _]): jStream[_] = {
    Invoke.qseq(map).asInstanceOf[clojure.lang.ASeq].stream()
  }

  // Query as data structure
  @varargs
  def qseq(query: jList[_], db: Db, args: Any*): jStream[_] = {
    qseq(Util.map(
      read(":query"), edn(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }

  // Query as String
  @varargs
  def qseq(query: String, db: Db, args: Any*): jStream[_] = {
    qseq(Util.map(
      read(":query"), read(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }
}
