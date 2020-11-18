package datomicClojure

import java.util.{Date, List => jList, Map => jMap}
import clojure.lang.IFn
import com.amazonaws.auth.AWSCredentialsProviderChain
import datomic.Util.read


object Invoke extends Invoke {
  val fn: String => IFn = (method: String) => syncFn(method)
}

object InvokeAsync extends Invoke {
  val fn: String => IFn = (method: String) => datomicAsyncFn(method)
}

trait Invoke extends ClojureBridge {
  // sync/async fn call
  val fn: String => IFn

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

  private def longOpt(key: String, opt: Option[Long]): String =
    opt.fold("")(n => s":$key $n")


  def administerSystem(
    datomicClient: AnyRef,
    options: jMap[_, _]
  ): jMap[_, _] = {
    fn("administer-system")
      .invoke(datomicClient, edn(options))
      .asInstanceOf[jMap[_, _]]
  }


  def asOf(datomicDb: AnyRef, t: Long): AnyRef = fn("as-of").invoke(datomicDb, t)

  def asOf(datomicDb: AnyRef, d: Date): AnyRef = fn("as-of").invoke(datomicDb, d)

  def clientCloudAWS(
    region: String,
    system: String,
    endpoint: String,
    credsProvider: AWSCredentialsProviderChain,
    proxyPort: Int
  ): AnyRef = {
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
  ): AnyRef = {
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
  ): AnyRef = {
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
  ): AnyRef = {
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
  ): AnyRef = {
    // Returns a connection
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
  ): AnyRef = {
    // Returns true or anomaly
    fn("create-database").invoke(
      datomicClient,
      read(s"""{:db-name "$dbName"${timeoutOpt(timeout)}}""")
    )
  }


  def datoms(
    datomicDb: AnyRef,
    index: String,
    components: jList[_]
  ): AnyRef = {
    fn("datoms").invoke(
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
    fn("db").invoke(datomicConn)
  }


  def dbStats(datomicDb: AnyRef): AnyRef = {
    fn("db-stats").invoke(datomicDb)
  }


  def deleteDatabase(
    datomicClient: AnyRef,
    dbName: String,
    timeout: Int = 0
  ): AnyRef = {
    // Returns true or anomaly
    fn("delete-database").invoke(
      datomicClient,
      read(s"""{:db-name "$dbName"${timeoutOpt(timeout)}}""")
    )
  }


  def history(datomicDb: AnyRef): AnyRef = {
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
  ): AnyRef = {
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
  ): AnyRef = {
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
  ): AnyRef = {
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


  def q(map: jMap[_, _]): AnyRef = fn("q").invoke(map)


  def qseq(map: jMap[_, _]): AnyRef = fn("qseq").invoke(map)


  def since(datomicDb: AnyRef, t: Long): AnyRef = {
    fn("since").invoke(datomicDb, t)
  }
  def since(datomicDb: AnyRef, d: Date): AnyRef = {
    fn("since").invoke(datomicDb, d)
  }


  def sync(datomicConn: AnyRef, t: Long): AnyRef = {
    fn("sync").invoke(datomicConn, t)
  }


  def transact(
    datomicConn: AnyRef,
    stmts: jList[_]
  ): AnyRef = {
    fn("transact").invoke(
      datomicConn,
      read(s"{:tx-data ${edn(stmts)}}")
    )
  }


  def txRange(
    datomicConn: AnyRef,
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): AnyRef = {
    fn("tx-range").invoke(
      datomicConn,
      read(
        s"""{
           |${longOpt("start", start)}
           |${longOpt("end", end)}
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
  ): AnyRef = {
    fn("with").invoke(
      withDb,
      read(s"{:tx-data ${edn(stmts)}}")
    )
  }

  def withDb(
    datomicConn: AnyRef
  ): AnyRef = {
    fn("with-db").invoke(datomicConn)
  }
}
