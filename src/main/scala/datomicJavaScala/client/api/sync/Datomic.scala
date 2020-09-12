package datomicJavaScala.client.api.sync

import java.util.stream.{Stream => jStream}
import java.util.{Collection => jCollection, List => jList, Map => jMap}
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomicJavaScala.util.ClojureBridge
import datomic.Util._


object Datomic extends ClojureBridge {

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
    syncFn("client").invoke(
      read(
        s"""{
           |:server-type :cloud
           |:region "$region"
           |:system "$system"
           |:endpoint "$endpoint"
           |:creds-provider $credsProvider
           |:proxy-port $proxyPort
           |}""".stripMargin)
    ).asInstanceOf[datomic.client.api.protocols.Client]
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
    syncFn("client").invoke(
      read(
        s"""{
           |:server-type :cloud
           |:region "$region"
           |:system "$system"
           |:endpoint "$endpoint"
           |:creds-profile "$credsProfile"
           |:proxy-port $proxyPort
           |}""".stripMargin)
    ).asInstanceOf[datomic.client.api.protocols.Client]
  )


  def clientForDevLocal(
    system: String,
    storageDir: Option[String] = None // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): Client = {
    val storage = storageDir.fold("")(sd => s""":storage-dir "$sd"""".stripMargin)
    Client(
      false,
      syncFn("client").invoke(
        read(
          s"""{
             |:server-type :dev-local
             |:system "$system"
             |$storage}""".stripMargin)
      ).asInstanceOf[datomic.client.api.protocols.Client]
    )
  }


  def clientForPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean = false
  ): Client = Client(
    true,
    syncFn("client").invoke(
      read(
        s"""{
           |:server-type :peer-server
           |:access-key "$accessKey"
           |:secret "$secret"
           |:endpoint "$endpoint"
           |:validate-hostnames $validateHostnames
           |}""".stripMargin)
    ).asInstanceOf[datomic.client.api.protocols.Client]
  )


  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def q(map: jMap[_, _]): jCollection[_] =
    syncFn("q").invoke(map).asInstanceOf[jCollection[_]]

  // Query as data structure
  def q(query: jList[_], db: Db, args: Any*): jCollection[_] =
    q(edn(query), db, args: _*)

  // Query as String
  def q(query: String, db: Db, args: Any*): jCollection[_] = {
    val result = args.size match {
      case 0  => syncFn("q").invoke(query, db.datomicDb)
      case 1  => syncFn("q").invoke(query, db.datomicDb, args(0))
      case 2  => syncFn("q").invoke(query, db.datomicDb, args(0), args(1))
      case 3  => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2))
      case 4  => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3))
      case 5  => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4))
      case 6  => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5))
      case 7  => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6))
      case 8  => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))
      case 9  => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8))
      case 10 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9))
      case 11 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10))
      case 12 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11))
      case 13 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12))
      case 14 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13))
      case 15 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14))
      case 16 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15))
      case 17 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16))
      case 18 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17))
      case 19 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18))
      case 20 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19))
      case 21 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20))
      case 22 => syncFn("q").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20), args(21))
    }
    result.asInstanceOf[jCollection[_]]
  }



  // Query as data structure or String + optional :offset, :limit, :timeout params
  // (see tests)
  def qseq(map: jMap[_, _]): jStream[_] =
    syncFn("qseq").invoke(map).asInstanceOf[clojure.lang.ASeq].stream()

  // Query as data structure
  def qseq(query: jList[_], db: Db, args: Any*): jStream[_] =
    qseq(edn(query), db, args: _*)

  def qseq(query: String, db: Db, args: Any*): jStream[_] = {
//    println("result...")
    val result = args.size match {
      case 0  => syncFn("qseq").invoke(query, db.datomicDb)
      case 1  => syncFn("qseq").invoke(query, db.datomicDb, args(0))
      case 2  => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1))
      case 3  => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2))
      case 4  => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3))
      case 5  => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4))
      case 6  => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5))
      case 7  => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6))
      case 8  => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))
      case 9  => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8))
      case 10 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9))
      case 11 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10))
      case 12 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11))
      case 13 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12))
      case 14 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13))
      case 15 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14))
      case 16 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15))
      case 17 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16))
      case 18 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17))
      case 19 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18))
      case 20 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19))
      case 21 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20))
      case 22 => syncFn("qseq").invoke(query, db.datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20), args(21))
    }

//    println("result: " + result)
//    types(result)

    result.asInstanceOf[clojure.lang.ASeq].stream()
  }
}
