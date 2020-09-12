package datomicJavaScala.client.api.async

import java.util.stream.{Stream => jStream}
import java.util.{List => jList, Map => jMap}
import clojure.lang.LazySeq
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomic.Util
import datomic.Util._
import datomicJavaScala.util.ClojureBridge


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
    storageDir: Option[String] = None // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): AsyncClient = {
    val storage = storageDir.fold("")(sd => s""":storage-dir "$sd"""".stripMargin)
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
    validateHostnames: Boolean = false
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

  // Query as data structure
  def q(query: jList[_], db: AsyncDb, args: Any*): Channel[jStream[_]] =
    q(edn(query), db, args: _*)

  // Query as String
  def q(query: String, db: AsyncDb, args: Any*): Channel[jStream[_]] = args.size match {
    case 0  => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb)))
    case 1  => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0))))
    case 2  => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1))))
    case 3  => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2))))
    case 4  => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3))))
    case 5  => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4))))
    case 6  => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5))))
    case 7  => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6))))
    case 8  => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))))
    case 9  => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8))))
    case 10 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9))))
    case 11 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10))))
    case 12 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11))))
    case 13 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12))))
    case 14 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13))))
    case 15 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14))))
    case 16 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15))))
    case 17 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16))))
    case 18 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17))))
    case 19 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18))))
    case 20 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19))))
    case 21 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20))))
    case 22 => q(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20), args(21))))
  }


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def qseq(map: jMap[_, _]): Channel[jStream[_]] = {
    new Channel[jStream[_]](
      datomicAsyncFn("qseq").invoke(map),
      Some((res: AnyRef) => res.asInstanceOf[LazySeq].stream)
    )
  }

  // Query as data structure
  def qseq(query: jList[_], db: AsyncDb, args: Any*): Channel[jStream[_]] =
    qseq(edn(query), db, args: _*)

  def qseq(query: String, db: AsyncDb, args: Any*): Channel[jStream[_]] = args.size match {
    case 0  => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb)))
    case 1  => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0))))
    case 2  => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1))))
    case 3  => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2))))
    case 4  => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3))))
    case 5  => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4))))
    case 6  => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5))))
    case 7  => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6))))
    case 8  => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))))
    case 9  => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8))))
    case 10 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9))))
    case 11 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10))))
    case 12 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11))))
    case 13 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12))))
    case 14 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13))))
    case 15 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14))))
    case 16 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15))))
    case 17 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16))))
    case 18 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17))))
    case 19 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18))))
    case 20 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19))))
    case 21 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20))))
    case 22 => qseq(Util.map(read(":query"), query, read(":args"), list(db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20), args(21))))
  }
}
