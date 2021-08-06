package datomicClient

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, OffsetDateTime, ZonedDateTime}
import java.util.{Date, List => jList, Map => jMap}
import clojure.lang.{IFn, PersistentHashMap}
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
    case Some(d: Date)   => s""":$key #inst "${date2datomicStr(d)}""""
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


  // API .......................................................................

  def administerSystem(
    datomicClient: AnyRef,
    options: jMap[_, _]
  ): jMap[_, _] = catchAnomaly {
    val args = edn(options)
    fn("administer-system").invoke(datomicClient, args).asInstanceOf[jMap[_, _]]
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
    val argsMap = read(
      s"""{
         |:server-type :cloud
         |:region "$region"
         |:system "$system"
         |:endpoint "$endpoint"
         |:creds-provider $credsProvider
         |:proxy-port $proxyPort
         |}""".stripMargin)
    fn("client").invoke(argsMap)
  }

  def clientCloudCredsProfile(
    region: String,
    system: String,
    endpoint: String,
    credsProfile: String,
    proxyPort: Int
  ): AnyRef = catchAnomaly {
    val argsMap = read(
      s"""{
         |:server-type :cloud
         |:region "$region"
         |:system "$system"
         |:endpoint "$endpoint"
         |:creds-profile "$credsProfile"
         |:proxy-port $proxyPort
         |}""".stripMargin)
    fn("client").invoke(argsMap)
  }

  def clientDevLocal(
    system: String,
    storageDir: String // overrides :storage-dir in ~/.datomic/dev-local.edn
  ): AnyRef = catchAnomaly {
    val storage = if (storageDir.nonEmpty)
      s""":storage-dir "$storageDir"""".stripMargin else ""
    val argsMap = read(
      s"""{
         |:server-type :dev-local
         |:system "$system"
         |$storage
         |}""".stripMargin)
    fn("client").invoke(argsMap)
  }

  def clientPeerServer(
    accessKey: String,
    secret: String,
    endpoint: String,
    validateHostnames: Boolean
  ): AnyRef = catchAnomaly {
    fn("client").invoke(
      PersistentHashMap.createWithCheck(
        read(":server-type"), read(":peer-server"),
        read(":access-key"), accessKey,
        read(":secret"), secret,
        read(":endpoint"), endpoint,
        read(":validate-hostnames"), validateHostnames.asInstanceOf[Object]
      )
    )
  }

  def connect(
    datomicClient: AnyRef,
    dbName: String,
    timeout: Int = 0
  ): AnyRef = catchAnomaly {
    val argsMap = read(
      s"""{
         |:db-name "$dbName"
         |${timeoutOpt(timeout)}
         |}""".stripMargin)
    // Returns a connection or throws
    fn("connect").invoke(datomicClient, argsMap)
  }


  def createDatabase(
    datomicClient: AnyRef,
    dbName: String,
    timeout: Int = 0
  ): AnyRef = catchAnomaly {
    val argsMap = read(s"""{:db-name "$dbName"${timeoutOpt(timeout)}}""")
    // Returns true or anomaly
    fn("create-database").invoke(datomicClient, argsMap)
  }


  def datoms(
    datomicDb: AnyRef,
    index: String,
    componentsList: jList[_],
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): AnyRef = catchAnomaly {
    val components = if (componentsList.isEmpty) "" else
      ":components " + edn(componentsList)
    val argsMap    = read(
      s"""{
         |:index $index
         |$components
         |${timeoutOpt(timeout)}
         |${offsetOpt(offset)}
         |${limitOpt(limit)}
         |}""".stripMargin
    )
    fn("datoms").invoke(datomicDb, argsMap)
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
    val argsMap = read(s"""{:db-name "$dbName"${timeoutOpt(timeout)}}""")
    // Returns true or anomaly
    fn("delete-database").invoke(datomicClient, argsMap)
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
  ): AnyRef = {
    if (!Seq(":avet", ":aevt").contains(index))
      throw new IllegalArgumentException(ErrorMsg.indexPull)

    catchAnomaly {
      val reverse_ = if (reverse) ":reverse true" else ""
      val argsMap  = read(
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
      fn("index-pull").invoke(datomicDb, argsMap)
    }
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
    val argsMap = read(
      s"""{
         |:attrid $attrId
         |${anyOpt("start", start)}
         |${anyOpt("end", end)}
         |${timeoutOpt(timeout)}
         |${offsetOpt(offset)}
         |${limitOpt(limit)}
         |}""".stripMargin
    )
    fn("index-range").invoke(datomicDb, argsMap)
  }


  def listDatabase(
    datomicClient: AnyRef,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): AnyRef = catchAnomaly {
    val argsMap = read(
      s"""{
         |${timeoutOpt(timeout)}
         |${offsetOpt(offset)}
         |${limitOpt(limit)}
         |}""".stripMargin
    )
    fn("list-databases").invoke(datomicClient, argsMap)
  }


  def pull(
    datomicDb: AnyRef,
    selector: String,
    eid0: Any,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): AnyRef = catchAnomaly {
    val eid     = eid0 match {
      case l: jList[_] => edn(l)
      case number      => number
    }
    val argsMap = read(
      s"""{
         |:selector $selector
         |:eid $eid
         |${timeoutOpt(timeout)}
         |${offsetOpt(offset)}
         |${limitOpt(limit)}
         |}""".stripMargin
    )
    fn("pull").invoke(datomicDb, argsMap)
  }


  def q(map: jMap[_, _]): AnyRef = catchAnomaly {
    fn("q").invoke(PersistentHashMap.create(map))
  }


  def qseq(map: jMap[_, _]): AnyRef = catchAnomaly {
    fn("qseq").invoke(PersistentHashMap.create(map))
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
    val txData  = edn(stmts)
    // clojure.tools.reader/read-string needed to interpret uri representation.
    val argsMap = readString(s"{:tx-data $txData}")
    //    val argsMap = read(s"{:tx-data $txData}")
    fn("transact").invoke(datomicConn, argsMap)
  }


  def txRange(
    datomicConn: AnyRef,
    start: Option[Any] = None,
    end: Option[Any] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): AnyRef = catchAnomaly {
    val argsMap = read(
      s"""{
         |${timePointOpt("start", start)}
         |${timePointOpt("end", end)}
         |${timeoutOpt(timeout)}
         |${offsetOpt(offset)}
         |${limitOpt(limit)}
         |}""".stripMargin)
    fn("tx-range").invoke(datomicConn, argsMap)
  }


  def `with`(
    withDb: AnyRef,
    stmts: jList[_]
  ): AnyRef = catchAnomaly {
    val txData  = edn(stmts)
    val argsMap = readString(s"{:tx-data $txData}")
    fn("with").invoke(withDb, argsMap)
  }

  def withDb(
    datomicConn: AnyRef
  ): AnyRef = catchAnomaly {
    fn("with-db").invoke(datomicConn)
  }
}
