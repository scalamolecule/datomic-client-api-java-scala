package datomicJava.client.api.async

import java.util.concurrent.CompletableFuture
import java.util.stream.Stream
import java.util.{List => jList, Map => jMap}
import clojure.lang.LazySeq
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomic.Util
import datomic.Util._
import datomicClient._
import scala.annotation.varargs


object AsyncDatomic extends ClojureBridge {

  require("clojure.core.async")
  require("cognitect.anomalies")
  require("datomic.client.api.async")

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
    storageDir: String // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): AsyncClient = {
    AsyncClient(
      false,
      InvokeAsync.clientDevLocal(system, storageDir)
    )
  }

  def clientDevLocal(
    system: String
  ): AsyncClient = clientDevLocal(system, "")


  def clientPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean
  ): AsyncClient = AsyncClient(
    true,
    InvokeAsync.clientPeerServer(accessKey, secret, endpoint, validateHostnames)
  )

  def clientPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
  ): AsyncClient = clientPeerServer(accessKey, secret, endpoint, false)


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def q(argMap: jMap[_, _])
  : CompletableFuture[Channel[Stream[_]]] = {
    CompletableFuture.supplyAsync { () =>
      Channel[Stream[_]](
        InvokeAsync.q(argMap),
        Some((res: AnyRef) => res.asInstanceOf[LazySeq].stream)
      )
    }
  }

  // Query as data structure
  @varargs
  def q(query: jList[_], db: AsyncDb, args: Any*)
  : CompletableFuture[Channel[Stream[_]]] = {
    q(Util.map(
      read(":query"), edn(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }

  // Query as String
  @varargs
  def q(query: String, db: AsyncDb, args: Any*)
  : CompletableFuture[Channel[Stream[_]]] = {
    q(Util.map(
      read(":query"), read(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def qseq(map: jMap[_, _])
  : CompletableFuture[Channel[Stream[_]]] = {
    CompletableFuture.supplyAsync { () =>
      Channel[Stream[_]](
        datomicAsyncFn("qseq").invoke(map),
        Some((res: AnyRef) => res.asInstanceOf[LazySeq].stream)
      )
    }
  }

  // Query as data structure
  @varargs
  def qseq(query: jList[_], db: AsyncDb, args: Any*)
  : CompletableFuture[Channel[Stream[_]]] = {
    qseq(Util.map(
      read(":query"), edn(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }

  // Query as String
  @varargs
  def qseq(query: String, db: AsyncDb, args: Any*)
  : CompletableFuture[Channel[Stream[_]]] = {
    qseq(Util.map(
      read(":query"), read(query),
      read(":args"), list(db.datomicDb +: args: _*)
    ))
  }
}
