package datomicJava.client.api.async

import java.util.stream.{Stream => jStream}
import java.util.{List => jList, Map => jMap}
import clojure.lang.LazySeq
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomic.Util
import datomic.Util._
import datomicJava.util.Helper._


object AsyncDatomic {

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
    datomicAsyncFn("client").invoke(
      read(
        s"""{
           |:server-type :cloud
           |:region "$region"
           |:system "$system"
           |:endpoint "$endpoint"
           |:creds-provider $credsProvider
           |:proxy-port $proxyPort
           |}""".stripMargin)
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
    datomicAsyncFn("client").invoke(
      read(
        s"""{
           |:server-type :cloud
           |:region "$region"
           |:system "$system"
           |:endpoint "$endpoint"
           |:creds-profile "$credsProfile"
           |:proxy-port $proxyPort
           |}""".stripMargin)
    )
  )


  def clientForDevLocal(
    system: String,
  ): AsyncClient = clientForDevLocal(system, "")

  def clientForDevLocal(
    system: String,
    storageDir: String // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): AsyncClient = {
    val storage = if(storageDir.nonEmpty)
      s""":storage-dir "$storageDir"""".stripMargin else ""
    AsyncClient(
      false,
      datomicAsyncFn("client").invoke(
        read(
          s"""{
             |:server-type :dev-local
             |:system "$system"
             |$storage}""".stripMargin)
      )
    )
  }



  def clientForPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
  ): AsyncClient = clientForPeerServer(accessKey, secret, endpoint, false)

  def clientForPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean
  ): AsyncClient = AsyncClient(
    true,
    datomicAsyncFn("client").invoke(
      read(
        s"""{
           |:server-type :peer-server
           |:access-key "$accessKey"
           |:secret "$secret"
           |:endpoint "$endpoint"
           |:validate-hostnames $validateHostnames
           |}""".stripMargin)
    )
  )


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def q(map: jMap[_, _]): Channel[jStream[_]] = {
    new Channel[jStream[_]](
      datomicAsyncFn("q").invoke(map),
      Some((res: AnyRef) => res.asInstanceOf[LazySeq].stream)
    )
  }

  // Spelling out multiple arities to satisfy Scala/Java compatibility

  // Query as data structure
  def q(query: jList[_], db: AsyncDb): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb)))
  def q(query: jList[_], db: AsyncDb, a: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u)))
  def q(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any, v: Any): Channel[jStream[_]] = q(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u, v)))

  // Query as String
  def q(query: String, db: AsyncDb): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb)))
  def q(query: String, db: AsyncDb, a: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a)))
  def q(query: String, db: AsyncDb, a: Any, b: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u)))
  def q(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any, v: Any): Channel[jStream[_]] = q(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u, v)))


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def qseq(map: jMap[_, _]): Channel[jStream[_]] = {
    new Channel[jStream[_]](
      datomicAsyncFn("qseq").invoke(map),
      Some((res: AnyRef) => res.asInstanceOf[LazySeq].stream)
    )
  }

  // Spelling out multiple arities to satisfy Scala/Java compatibility

  // Query as data structure
  def qseq(query: jList[_], db: AsyncDb): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb)))
  def qseq(query: jList[_], db: AsyncDb, a: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u)))
  def qseq(query: jList[_], db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any, v: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), edn(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u, v)))

  // Query as String
  def qseq(query: String, db: AsyncDb): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb)))
  def qseq(query: String, db: AsyncDb, a: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u)))
  def qseq(query: String, db: AsyncDb, a: Any, b: Any, c: Any, d: Any, e: Any, f: Any, g: Any, h: Any, i: Any, j: Any, k: Any, l: Any, m: Any, n: Any, o: Any, p: Any, x: Any, r: Any, s: Any, t: Any, u: Any, v: Any): Channel[jStream[_]] = qseq(Util.map(read(":query"), read(query), read(":args"), list(db.datomicDb, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, x, r, s, t, u, v)))
}
