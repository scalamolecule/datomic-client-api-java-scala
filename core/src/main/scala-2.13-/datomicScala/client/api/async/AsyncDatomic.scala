package datomicScala.client.api.async

import java.util.stream.{Stream => jStream}
import java.util.{List => jList, Map => jMap, Collection => jCollection}
import cats.effect.IO
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomic.Util
import datomic.Util._
import datomicClient._
import datomicClient.anomaly.CognitectAnomaly
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object AsyncDatomic extends ClojureBridge {

  require("clojure.core.async")
  require("cognitect.anomalies")
  require("datomic.client.api.async")

  /*
  From the clojure documentation:
  https://docs.datomic.com/client-api/datomic.client.api.async.html#var-client

  Create a client for a Datomic system. This function does not
  communicate with a server and returns immediately.
  */

  // Providing AWSCredentialsProviderChain
  def clientCloud(
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
  def clientCloud(
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


  def clientDevLocal(
    system: String,
    storageDir: String = "" // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): AsyncClient = AsyncClient(
    false,
    InvokeAsync.clientDevLocal(system, storageDir)
  )


  def clientPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean = false
  ): AsyncClient = AsyncClient(
    true,
    InvokeAsync.clientPeerServer(accessKey, secret, endpoint, validateHostnames)
  )


  // Chunks from clojure Channels are returned asynchronously as Futures
  // of either a CognitectAnomaly or a java Stream of lists of data.

  // Query from map of data:
  //   :query
  //   :offset  (optional)
  //   :limit   (optional)
  //   :timeout (optional)
  //   :chunk   (optional)
  def q(argMap: jMap[_, _])
  : Future[Stream[Either[CognitectAnomaly, jStream[_]]]] = Future {
    Channel[jStream[_]](
      InvokeAsync.q(argMap),
      Some((res: AnyRef) => res.asInstanceOf[jCollection[_]].stream)
    ).lazyList
  }

  // Query as data structure
  def q(query: jList[_], db: AsyncDb, args: Any*)
  : Future[Stream[Either[CognitectAnomaly, jStream[_]]]] = {
    val args1 = args.toSeq.asInstanceOf[Seq[Object]]
    q(Util.map(
      read(":query"), edn(query),
      read(":args"), list(db.datomicDb +: args1: _*)
    ))
  }

  // Query as String
  def q(query: String, db: AsyncDb, args: Any*)
  : Future[Stream[Either[CognitectAnomaly, jStream[_]]]] = {
    val args1 = args.toSeq.asInstanceOf[Seq[Object]]
    q(Util.map(
      read(":query"), read(query),
      read(":args"), list(db.datomicDb +: args1: _*)
    ))
  }

  // fs2 Stream implementation example
  // This allows the first the first chunk to be lazy
  // Usage is flexible but verbose
  def qStream(argMap: jMap[_, _])
  : fs2.Stream[IO, Either[CognitectAnomaly, jStream[_]]] = {
    Channel[jStream[_]](
      InvokeAsync.q(argMap),
      Some((res: AnyRef) => res.asInstanceOf[jCollection[_]].stream)
    ).myTerminatedStream
  }


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def qseq(argMap: jMap[_, _])
  : Future[Stream[Either[CognitectAnomaly, jStream[_]]]] = Future {
    Channel[jStream[_]](
      InvokeAsync.qseq(argMap),
      Some((res: AnyRef) => res.asInstanceOf[jCollection[_]].stream)
    ).lazyList
  }

  // Query as data structure
  def qseq(query: jList[_], db: AsyncDb, args: Any*)
  : Future[Stream[Either[CognitectAnomaly, jStream[_]]]] = {
    val args1 = args.toSeq.asInstanceOf[Seq[Object]]
    qseq(Util.map(
      read(":query"), edn(query),
      read(":args"), list(db.datomicDb +: args1: _*)
    ))
  }

  // Query as String
  def qseq(query: String, db: AsyncDb, args: Any*)
  : Future[Stream[Either[CognitectAnomaly, jStream[_]]]] = {
    val args1 = args.toSeq.asInstanceOf[Seq[Object]]
    qseq(Util.map(
      read(":query"), read(query),
      read(":args"), list(db.datomicDb +: args1: _*)
    ))
  }
}
