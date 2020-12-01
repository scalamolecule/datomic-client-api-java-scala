package datomicClient

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, OffsetDateTime, ZonedDateTime}
import java.util
import java.util.{Date, List => jList, Map => jMap}
import clojure.lang.IFn
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomic.Util.read
import datomicClient.anomaly.AnomalyWrapper


object Invoke extends Invoke {
  val fn: String => IFn = (method: String) => syncFn(method)
}

object InvokeAsync extends Invoke {
  val fn: String => IFn = (method: String) => datomicAsyncFn(method)
}

trait Invoke extends ClojureBridge with AnomalyWrapper {

  // sync/async fn to be invoked
  val fn: String => IFn

  // Helper methods ............................................................

  private def positive(key: String, n: Int, default: Int = 0): String = n match {
    case `default`  => ""
    case n if n < 1 => throw new IllegalArgumentException(ErrorMsg.zeroNeg)
    case n          => s":$key $n"
  }
  private def timeoutOpt(timeout: Int): String = positive("timeout", timeout)
  private def offsetOpt(offset: Int): String = positive("offset", offset)

  private def limitOpt(limit: Int): String = limit match {
    case -1         => ":limit -1"
    case n if n < 1 => throw new IllegalArgumentException(ErrorMsg.limit)
    case n          => s":limit $n"
  }

  private def anyOpt(key: String, opt: Option[Any]): String = opt match {
    case None            => ""
    case Some(s: String) => s""":$key "${read(s)}""""
    case Some(v)         => s":$key $v"
  }

  private def date2datomicStr(date: Date): String = {
    val zoneOffset = OffsetDateTime.now().getOffset
    val pattern    = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    val ldt        = LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), zoneOffset)
    val zdt        = ZonedDateTime.of(ldt, zoneOffset)
    zdt.format(DateTimeFormatter.ofPattern(pattern))
  }

  private def timePointOpt(key: String, opt: Option[Any]): String = {
    def validNumber(n: Long): Long = {
      if (n < 1)
        throw new RuntimeException(s"Time point has to be > 0 (was $n)")
      n
    }
    opt.fold("") {
      case n: Int  => s":$key ${validNumber(n)}"
      case n: Long => s":$key ${validNumber(n)}"
      case d: Date => s""":$key #inst "${date2datomicStr(d)}""""
      case x       => throw new RuntimeException(
        s"Unexpected time point `$x` of type ${x.getClass}."
      )
    }
  }

  def keywordAware(components: jList[_]): jList[_] = {
    val aware: jList[Any] = new util.ArrayList[Any]()
    components.forEach(v => aware.add(read(v.toString)))
    aware
  }


  // API .......................................................................

  def administerSystem(
    datomicClient: AnyRef,
    options: jMap[_, _]
  ): jMap[_, _] = catchAnomaly {
    fn("administer-system")
      .invoke(datomicClient, edn(options))
      .asInstanceOf[jMap[_, _]]
  }


  def asOf(datomicDb: AnyRef, t: Long): AnyRef = catchAnomaly {
    fn("as-of").invoke(datomicDb, t)
  }

  def asOf(datomicDb: AnyRef, d: Date): AnyRef = catchAnomaly {
    fn("as-of").invoke(datomicDb, d)
  }

  def clientCloudAWS(
    region: String,
    system: String,
    endpoint: String,
    credsProvider: AWSCredentialsProviderChain,
    proxyPort: Int
  ): AnyRef = catchAnomaly {
    fn("client").invoke(
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
  }

  def clientCloudCredsProfile(
    region: String,
    system: String,
    endpoint: String,
    credsProfile: String,
    proxyPort: Int
  ): AnyRef = catchAnomaly {
    fn("client").invoke(
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
  }

  def clientDevLocal(
    system: String,
    storageDir: String // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): AnyRef = catchAnomaly {
    val storage = if (storageDir.nonEmpty)
      s""":storage-dir "$storageDir"""".stripMargin else ""
    fn("client").invoke(
      read(
        s"""{
           |:server-type :dev-local
           |:system "$system"
           |$storage
           |}""".stripMargin)
    )
  }

  def clientPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean
  ): AnyRef = catchAnomaly {
    fn("client").invoke(
      read(
        s"""{
           |:server-type :peer-server
           |:access-key "$accessKey"
           |:secret "$secret"
           |:endpoint "$endpoint"
           |:validate-hostnames $validateHostnames
           |}""".stripMargin)
    )
  }

  def connect(
    datomicClient: AnyRef,
    dbName: String,
    timeout: Int = 0
  ): AnyRef = catchAnomaly {
    // Returns a connection or throws
    fn("connect").invoke(
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
  ): AnyRef = catchAnomaly {
    // Returns true or anomaly
    fn("create-database").invoke(
      datomicClient,
      read(s"""{:db-name "$dbName"${timeoutOpt(timeout)}}""")
    )
  }


  def datoms(
    datomicDb: AnyRef,
    index: String,
    componentsList: jList[_]
  ): AnyRef = catchAnomaly {
    val components = edn(keywordAware(componentsList))
    fn("datoms").invoke(
      datomicDb,
      read(
        s"""{
           |:index $index
           |:components $components
           |}""".stripMargin
      )
    )
  }


  def db(datomicConn: AnyRef): AnyRef = catchAnomaly {
    fn("db").invoke(datomicConn)
  }


  def dbStats(datomicDb: AnyRef): AnyRef = catchAnomaly {
    fn("db-stats").invoke(datomicDb)
  }


  def deleteDatabase(
    datomicClient: AnyRef,
    dbName: String,
    timeout: Int = 0
  ): AnyRef = catchAnomaly {
    // Returns true or anomaly
    fn("delete-database").invoke(
      datomicClient,
      read(s"""{:db-name "$dbName"${timeoutOpt(timeout)}}""")
    )
  }


  def history(datomicDb: AnyRef): AnyRef = catchAnomaly {
    fn("history").invoke(datomicDb)
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
  ): AnyRef = catchAnomaly {
    val reverse_ = if (reverse) ":reverse true" else ""
    fn("index-pull").invoke(
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
    )
  }


  def indexRange(
    datomicDb: AnyRef,
    attrId: String,
    start: Option[Any] = None,
    end: Option[Any] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): AnyRef = catchAnomaly {
    fn("index-range").invoke(
      datomicDb,
      read(
        s"""{
           |:attrid $attrId
           |${anyOpt("start", start)}
           |${anyOpt("end", end)}
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
  ): AnyRef = catchAnomaly {
    fn("list-databases").invoke(
      datomicClient,
      read(
        s"""{
           |${timeoutOpt(timeout)}
           |${offsetOpt(offset)}
           |${limitOpt(limit)}
           |}""".stripMargin
      )
    )
  }


  def pull(
    datomicDb: AnyRef,
    selector: String,
    eid: Any,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): AnyRef = catchAnomaly {
    fn("pull").invoke(
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
    )
  }


  def q(map: jMap[_, _]): AnyRef = catchAnomaly {
    fn("q").invoke(map)
  }


  def qseq(map: jMap[_, _]): AnyRef = catchAnomaly {
    fn("qseq").invoke(map)
  }


  def since(datomicDb: AnyRef, t: Long): AnyRef = catchAnomaly {
    fn("since").invoke(datomicDb, t)
  }
  def since(datomicDb: AnyRef, d: Date): AnyRef = catchAnomaly {
    fn("since").invoke(datomicDb, d)
  }


  def sync(datomicConn: AnyRef, t: Long): AnyRef = catchAnomaly {
    fn("sync").invoke(datomicConn, t)
  }


  def transact(
    datomicConn: AnyRef,
    stmts: jList[_]
  ): AnyRef = catchAnomaly {
    val txData = edn(stmts)
    fn("transact").invoke(
      datomicConn,
      //      datomic.Util.map(read(":tx-data"), stmts)
      //      read(s"{:tx-data $txData}")

      // clojure.tools.reader/read-string needed to interpret uri representation
      // When maps of data are accepted (bug fixed), we can likely use `read` again
      readString(s"{:tx-data $txData}")
    )
  }


  def txRange(
    datomicConn: AnyRef,
    start: Option[Any] = None,
    end: Option[Any] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): AnyRef = catchAnomaly {
    fn("tx-range").invoke(
      datomicConn,
      read(
        s"""{
           |${timePointOpt("start", start)}
           |${timePointOpt("end", end)}
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
  ): AnyRef = catchAnomaly {
    val txData = edn(stmts)
    fn("with").invoke(
      withDb,
      read(s"{:tx-data $txData}")
    )
  }

  def withDb(
    datomicConn: AnyRef
  ): AnyRef = catchAnomaly {
    fn("with-db").invoke(datomicConn)
  }
}
