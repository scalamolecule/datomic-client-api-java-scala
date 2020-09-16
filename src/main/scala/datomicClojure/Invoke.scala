package datomicClojure

import java.util.stream.{Stream => jStream}
import java.util.{Collection => jCollection, List => jList, Map => jMap}
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomic.Util.read
import datomic.client.api.protocols
import datomicJava.util.Helper.syncFn


object Invoke extends ClojureBridge {

  private def timeoutOpt(timeout: Int): String = if (timeout == 0) "" else s":timeout $timeout"
  private def offsetOpt(offset: Int): String = if (offset == 0) "" else s":offset $offset"
  private def limitOpt(limit: Int): String = if (limit == 0) "" else s":limit $limit"
  private def startOpt(start: Option[Any]): String = start.fold("")(s => s":start $s")
  private def endOpt(end: Option[Any]): String = end.fold("")(s => s":end $s")


  // datomic.client.api invocations -------------------------------

  def administerSystem(
    datomicClient: AnyRef,
    options: String
  ): jMap[_, _] = {
    syncFn("administer-system")
      .invoke(datomicClient, read(options))
      .asInstanceOf[jMap[_, _]]
  }

  def asOf(
    datomicDb: AnyRef,
    t: Long
  ): AnyRef = {
    syncFn("as-of").invoke(datomicDb, t)
  }

  def clientCloudAWS(
    region: String,
    system: String,
    endpoint: String,
    credsProvider: AWSCredentialsProviderChain,
    proxyPort: Int
  ): protocols.Client = {
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
  }

  def clientCloudCredsProfile(
    region: String,
    system: String,
    endpoint: String,
    credsProfile: String,
    proxyPort: Int
  ): protocols.Client = {
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
  }

  def clientDevLocal(
    system: String,
    storageDir: String // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): protocols.Client = {
    val storage = if (storageDir.nonEmpty)
      s""":storage-dir "$storageDir"""".stripMargin else ""
    syncFn("client").invoke(
      read(
        s"""{
           |:server-type :dev-local
           |:system "$system"
           |$storage
           |}""".stripMargin)
    ).asInstanceOf[datomic.client.api.protocols.Client]
  }

  def clientPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean
  ): protocols.Client = {
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
  }

  def connect(
    datomicClient: AnyRef,
    dbName: String,
    timeout: Int = 0
  ): AnyRef = {
    // Returns a connection
    syncFn("connect").invoke(
      datomicClient,
      read(
        s"""{
           |:db-name "$dbName"
           |${timeoutOpt(timeout)}
           |}""".stripMargin)
    )
  }


  def createDatabase(
    datomicClient: AnyRef,
    dbName: String,
    timeout: Int = 0
  ): Boolean = {
    // Returns true or anomaly
    syncFn("create-database").invoke(
      datomicClient,
      read(s"""{:db-name "$dbName"${timeoutOpt(timeout)}}""")
    ).asInstanceOf[Boolean]
  }


  def datoms(
    datomicDb: AnyRef,
    index: String,
    components: jList[_]
  ): AnyRef = {
    syncFn("datoms").invoke(
      datomicDb,
      read(
        s"""{
           |:index $index
           |:components ${edn(components)}
           |}""".stripMargin
      )
    )
  }


  def db(datomicConn: AnyRef): AnyRef = {
    syncFn("db").invoke(datomicConn)
  }


  def dbStats(datomicDb: AnyRef): jMap[_, _] = {
    syncFn("db-stats").invoke(datomicDb).asInstanceOf[jMap[_, _]]
  }

  def deleteDatabase(
    datomicClient: AnyRef,
    dbName: String,
    timeout: Int = 0
  ): Boolean = {
    // Returns true or anomaly
    syncFn("delete-database").invoke(
      datomicClient,
      read(s"""{:db-name "$dbName"${timeoutOpt(timeout)}}""")
    ).asInstanceOf[Boolean]
  }


  def history(datomicDb: AnyRef): AnyRef = {
    syncFn("history").invoke(datomicDb)
  }


  def indexPull(
    datomicDb: AnyRef,
    index: String,
    selector: String,
    start: String,
    reverse: Boolean = false,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): jStream[_] = {
    val reverse_ = if(reverse) ":reverse true" else ""
    syncFn("index-pull").invoke(
      datomicDb,
      read(
        s"""{
           |:index $index
           |:selector $selector
           |:start $start
           |$reverse_
           |${timeoutOpt(timeout)}
           |${offsetOpt(offset)}
           |${limitOpt(limit)}
           |}""".stripMargin
      )
    ).asInstanceOf[clojure.lang.ASeq].stream()
  }


  def indexRange(
    datomicDb: AnyRef,
    attrId: String,
    start: Option[Any] = None,
    end: Option[Any] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): AnyRef = {
    syncFn("index-range").invoke(
      datomicDb,
      read(
        s"""{
           |:attrid $attrId
           |${startOpt(start)}
           |${endOpt(end)}
           |${timeoutOpt(timeout)}
           |${offsetOpt(offset)}
           |${limitOpt(limit)}
           |}""".stripMargin
      )
    )
  }


  def listDatabase(
    datomicClient: AnyRef,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): jList[String] = {
    syncFn("list-databases").invoke(
      datomicClient,
      read(
        s"""{
           |${timeoutOpt(timeout)}
           |${offsetOpt(offset)}
           |${limitOpt(limit)}
           |}""".stripMargin
      )
    ).asInstanceOf[jList[String]]
  }


  def pull(
    datomicDb: AnyRef,
    selector: String,
    eid: Any,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): jMap[_, _] = {
    syncFn("pull").invoke(
      datomicDb,
        read(
        s"""{
           |:selector $selector
           |:eid $eid
           |${timeoutOpt(timeout)}
           |${offsetOpt(offset)}
           |${limitOpt(limit)}
           |}""".stripMargin
        )
    ).asInstanceOf[jMap[_, _]]
  }

  def q(map: jMap[_, _]): jCollection[jList[AnyRef]] = {
    syncFn("q").invoke(map).asInstanceOf[jCollection[jList[AnyRef]]]
  }

  def q(query: String, datomicDb: AnyRef, args: Any*): jCollection[jList[AnyRef]] = {
    (args.size match {
      case 0  => syncFn("q").invoke(query, datomicDb)
      case 1  => syncFn("q").invoke(query, datomicDb, args(0))
      case 2  => syncFn("q").invoke(query, datomicDb, args(0), args(1))
      case 3  => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2))
      case 4  => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3))
      case 5  => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4))
      case 6  => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5))
      case 7  => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6))
      case 8  => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))
      case 9  => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8))
      case 10 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9))
      case 11 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10))
      case 12 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11))
      case 13 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12))
      case 14 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13))
      case 15 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14))
      case 16 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15))
      case 17 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16))
      case 18 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17))
      case 19 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18))
      case 20 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19))
      case 21 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20))
      case 22 => syncFn("q").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20), args(21))
    }).asInstanceOf[jCollection[jList[AnyRef]]]
  }


  def qseq(map: jMap[_, _]): jStream[_] = {
    syncFn("qseq").invoke(map).asInstanceOf[clojure.lang.ASeq].stream()
  }

  def qseq(query: String, datomicDb: AnyRef, args: Any*): jStream[_] = {
    (args.size match {
      case 0  => syncFn("qseq").invoke(query, datomicDb)
      case 1  => syncFn("qseq").invoke(query, datomicDb, args(0))
      case 2  => syncFn("qseq").invoke(query, datomicDb, args(0), args(1))
      case 3  => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2))
      case 4  => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3))
      case 5  => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4))
      case 6  => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5))
      case 7  => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6))
      case 8  => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))
      case 9  => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8))
      case 10 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9))
      case 11 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10))
      case 12 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11))
      case 13 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12))
      case 14 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13))
      case 15 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14))
      case 16 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15))
      case 17 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16))
      case 18 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17))
      case 19 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18))
      case 20 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19))
      case 21 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20))
      case 22 => syncFn("qseq").invoke(query, datomicDb, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16), args(17), args(18), args(19), args(20), args(21))
    }).asInstanceOf[clojure.lang.ASeq].stream()
  }


  def since(datomicDb: AnyRef, t: Long): AnyRef = {
    syncFn("since").invoke(datomicDb, t)
  }


  def sync(datomicConn: AnyRef, t: Long): AnyRef = {
    syncFn("sync").invoke(datomicConn, t)
  }


  def transact(
    datomicConn: AnyRef,
    stmts: jList[_]
  ): jMap[_, _] = {
    syncFn("transact").invoke(
      datomicConn,
      read(s"{:tx-data ${edn(stmts)}}")
    ).asInstanceOf[jMap[_, _]]
  }



  def txRange(
    datomicConn: AnyRef,
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): AnyRef = {
    syncFn("tx-range").invoke(
      datomicConn,
      read(
        s"""{
           |${startOpt(start)}
           |${endOpt(end)}
           |${timeoutOpt(timeout)}
           |${offsetOpt(offset)}
           |${limitOpt(limit)}
           |}""".stripMargin
      )
    )
  }


  def `with`(
    withDb: AnyRef,
    stmts: jList[_]
  ): jMap[_, _] = {
    syncFn("with").invoke(
      withDb,
      read(s"{:tx-data ${edn(stmts)}}")
    ).asInstanceOf[jMap[_, _]]
  }

  def withDb(
    datomicConn: AnyRef
  ): AnyRef = {
    syncFn("with-db").invoke(datomicConn)
  }
}
