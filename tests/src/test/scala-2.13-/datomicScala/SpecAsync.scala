package datomicScala

import java.util.{Date, stream, List => jList}
import clojure.lang.PersistentVector
import datomic.Util.list
import datomicClient.anomaly.CognitectAnomaly
import datomicScala.client.api.async._
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import org.specs2.specification.core.{Fragments, Text}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


trait SpecAsync extends Specification with SchemaAndData {
  sequential

  var system    : String          = "Not set yet. Can be: dev-local / peer-server / cloud"
  var client    : AsyncClient     = null // set in setup
  var conn      : AsyncConnection = null // set in setup
  var filmDataTx: AsyncTxReport   = null // set in setup

  var setupException = Option.empty[Throwable]

  // Convenience await (name 'await' is already used by specs2)
  def waitFor[T](body: => Future[T]): T = Await.result(body, Duration.Inf)

  def addSystem(fs: => Fragments, system: String) = fs.mapDescription {
    case Text(t)    => Text(s"$system        $t")
    case otherDescr => otherDescr
  }

  // todo: awaiting to find a way to invoke data as maps against Peer Server
  override def map(fs: => Fragments): Fragments =
      step(setupDevLocal()) ^ addSystem(fs, "dev-local  ") ^
      step(setupPeerServer()) ^ addSystem(fs, "peer-server")


  def setupDevLocal(): Unit = {
    system = "dev-local"
    client = AsyncDatomic.clientDevLocal("test-datomic-client-api-scala-2.12")
  }


  def setupPeerServer(): Unit = {
    system = "peer-server"
    try {
      client = AsyncDatomic.clientPeerServer("k", "s", "localhost:8998")
    } catch {
      case t: Throwable =>
        // Catch error from setup (suppressed during setup)
        setupException = Some(t)
    }
  }


  class AsyncSetup extends SchemaAndData with Scope {

    // Throw potential setup error
    setupException.fold()(throw _)

    var txBefore    : Long = 0L
    var txInstBefore: Date = null

    val isDevLocal = system == "dev-local"

    if (isDevLocal) {
      // Re-create db
      waitFor(client.deleteDatabase("hello"))
      waitFor(client.deleteDatabase("world"))
      waitFor(client.createDatabase("hello"))
      conn = waitFor(client.connect("hello")).toOption.get
      // Schema
      val schemaTx = waitFor(conn.transact(schema(false))).toOption.get
      txBefore = schemaTx.tx
      txInstBefore = schemaTx.txInst
      // Data
      filmDataTx = waitFor(conn.transact(filmData)).toOption.get

    } else {

      // Using the db associated with the Peer Server connection
      conn = waitFor(client.connect("hello")).toOption.get

      // Install schema if necessary
      if (waitFor(AsyncDatomic.q(
        "[:find ?e :where [?e :db/ident :movie/title]]",
        conn.db
      )).head.toOption.get.count() == 0) {
        println("Installing Peer Server hello db schema...")
        waitFor(conn.transact(schema(true))).toOption.get
      }

      // Retract current data
      var lastTx: AsyncTxReport = null
      waitFor(AsyncDatomic.q("[:find ?e :where [?e :movie/title _]]", conn.db))
        .head match {
        case Right(stream) => stream.forEach { l =>
          val eid: Any = l.asInstanceOf[jList[_]].get(0)
          lastTx = waitFor(
            conn.transact(list(list(":db/retractEntity", eid.asInstanceOf[Object])))
          ).toOption.get
        }
        case Left(anomaly) => throw anomaly
      }
      txBefore = lastTx.tx
      txInstBefore = lastTx.txInst

      filmDataTx = waitFor(conn.transact(filmData)).toOption.get
    }

    lazy val dbAfter     = filmDataTx.dbAfter
    lazy val tBefore     = filmDataTx.basisT
    lazy val tAfter      = filmDataTx.t
    lazy val txAfter     = filmDataTx.tx
    lazy val txInstAfter = filmDataTx.txInst

    private var films = List.empty[Long]
    filmDataTx.txData.skip(1).forEach(d => films = films :+ d.e)
    lazy val List(e1, e2, e3) = films.distinct.sorted

    // Ids of the three attributes
    val List(a1, a2, a3) = if (system == "dev-local")
      List(73, 74, 75) else List(72, 73, 74)

    def films(db: AsyncDb): Seq[String] = {
      val lazyList: Stream[Either[CognitectAnomaly, stream.Stream[_]]] = waitFor(AsyncDatomic.q(filmQuery, db))
      if (lazyList.nonEmpty) {
        // Avoid scala.jdk.CollectionConverters._ to work with both scala 2.12 and 2.13
        var films = Seq.empty[String]
        lazyList.head.toOption.get.forEach(f => films = films :+ f.asInstanceOf[PersistentVector].get(0).toString)
        films.sorted
      } else Nil
    }
  }
}