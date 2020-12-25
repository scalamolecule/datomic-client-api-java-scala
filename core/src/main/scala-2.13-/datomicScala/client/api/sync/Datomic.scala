package datomicScala.client.api.sync

import java.util.{Collection => jCollection, List => jList, Map => jMap}
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomic.Util
import datomic.Util.{list, read}
import datomicClient.anomaly.AnomalyWrapper
import datomicClient.{ClojureBridge, Invoke}
//import scala.jdk.StreamConverters._
import scala.collection.JavaConverters._

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
    storageDir: String = "" // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): Client = Client(
    false,
    Invoke.clientDevLocal(system, storageDir)
  )


  def clientPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean = false
  ): Client = Client(
    true,
    Invoke.clientPeerServer(accessKey, secret, endpoint, validateHostnames)
  )


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def q(argMap: jMap[_, _]): jCollection[jList[AnyRef]] = {
    Invoke.q(argMap).asInstanceOf[jCollection[jList[AnyRef]]]
  }

  // Query as data structure
  def q(query: jList[_], db: Db, args: Any*): jCollection[jList[AnyRef]] = {
    val args1 = args.toSeq.asInstanceOf[Seq[Object]]
    q(Util.map(
      read(":query"), edn(query),
      read(":args"), list(db.datomicDb +: args1: _*)
    ))
  }

  // Query as String
  def q(query: String, db: Db, args: Any*): jCollection[jList[AnyRef]] = {
    val args1 = args.toSeq.asInstanceOf[Seq[Object]]
    q(Util.map(
      read(":query"), read(query),
      read(":args"), list(db.datomicDb +: args1: _*)
    ))
  }


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  // (consuming all elements of java stream, so no real reason to use Stream type in scala 2.12)
  def qseq(argMap: jMap[_, _]): Stream[Any] = {
    Invoke.qseq(argMap).asInstanceOf[clojure.lang.ASeq].stream().iterator().asScala.toStream
  }

  // Query as data structure
  def qseq(query: jList[_], db: Db, args: Any*): Stream[Any] = {
    val args1 = args.toSeq.asInstanceOf[Seq[Object]]
    qseq(Util.map(
      read(":query"), edn(query),
      read(":args"), list(db.datomicDb +: args1: _*)
    ))
  }

  def qseq(query: String, db: Db, args: Any*): Stream[Any] = {
    val args1 = args.toSeq.asInstanceOf[Seq[Object]]
    qseq(Util.map(
      read(":query"), read(query),
      read(":args"), list(db.datomicDb +: args1: _*)
    ))
  }
}
