package datomicScala.client.api.async

import java.util.stream.{Stream => jStream}
import java.util.{List => jList, Map => jMap}
import clojure.lang.LazySeq
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomic.Util
import datomic.Util._
import datomicClojure.{ClojureBridge, InvokeAsync}

object AsyncDatomic extends ClojureBridge {

  require("clojure.core.async")
  require("cognitect.anomalies")
  require("datomic.client.api.async")

  // Providing AWSCredentialsProviderChain
  def clientForCloud(
    region: String,
    system: String,
    endpoint: String,
    credsProvider: AWSCredentialsProviderChain,
    proxyPort: Int
  ): AsyncClient = AsyncClient(
    false,
    InvokeAsync.clientCloudAWS(
      region, system, endpoint, credsProvider, proxyPort
    )
  )

  // Providing creds-profile name
  def clientForCloud(
    region: String,
    system: String,
    endpoint: String,
    credsProfile: String,
    proxyPort: Int
  ): AsyncClient = AsyncClient(
    false,
    InvokeAsync.clientCloudCredsProfile(
      region, system, endpoint, credsProfile, proxyPort
    )
  )


  def clientForDevLocal(
    system: String,
    storageDir: String = "" // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): AsyncClient = {
    AsyncClient(
      false,
      InvokeAsync.clientDevLocal(system, storageDir)
    )
  }


  def clientForPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean = false
  ): AsyncClient = AsyncClient(
    true,
    InvokeAsync.clientPeerServer(accessKey, secret, endpoint, validateHostnames)
  )


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def q(argMap: jMap[_, _]): Channel[jStream[_]] = {
    Channel[jStream[_]](
      InvokeAsync.q(argMap),
      Some((res: AnyRef) => res.asInstanceOf[LazySeq].stream)
    )
  }


  // Query as data structure
  def q(query: jList[_], db: AsyncDb, args: Any*): Channel[jStream[_]] = {
    q(Util.map(
      read(":query"), edn(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }

  // Query as String
  def q(query: String, db: AsyncDb, args: Any*): Channel[jStream[_]] = {
    q(Util.map(
      read(":query"), read(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }

  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def qseq(argMap: jMap[_, _]): Channel[jStream[_]] = {
    Channel[jStream[_]](
      InvokeAsync.qseq(argMap),
      Some((res: AnyRef) => res.asInstanceOf[LazySeq].stream)
    )
  }

  // Query as data structure
  def qseq(query: jList[_], db: AsyncDb, args: Any*): Channel[jStream[_]] = {
    qseq(Util.map(
      read(":query"), edn(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }

  // Query as String
  def qseq(query: String, db: AsyncDb, args: Any*): Channel[jStream[_]] = {
    qseq(Util.map(
      read(":query"), read(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }
}
