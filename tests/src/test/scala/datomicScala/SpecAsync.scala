package datomicScala

import java.util.{Date, List => jList}
import clojure.lang.PersistentVector
import datomic.Peer
import datomic.Util.list
import datomicScala.client.api.async._
import datomicScala.client.api.sync.Datomic
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import org.specs2.specification.core.{Fragments, Text}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._


trait SpecAsync extends Specification with SchemaAndData {

  var system  : String          = "Not set yet. Can be: dev-local / peer-server / cloud"
  var client  : AsyncClient     = null // set in setup
  var conn    : AsyncConnection = null // set in setup
  var txReport: AsyncTxReport   = null // set in setup

  // Convenience await (name 'await' is already used by specs2)
  def waitFor[T](body: => Future[T]): T = Await.result(body, Duration.Inf)

  // todo: awaiting to find a way to invoke data as maps against Peer Server
  override def map(fs: => Fragments): Fragments =
    step(setupDevLocal()) ^
      fs.mapDescription(d => Text(s"$system: " + d.show))
  //          fs.mapDescription(d => Text(s"$system: " + d.show)) ^
  //          step(setupPeerServer()) ^
  //          fs.mapDescription(d => Text(s"$system: " + d.show))


  def setupDevLocal(): Unit = {
    system = "dev-local"
    client = AsyncDatomic.clientDevLocal("Hello system name")
  }


  def setupPeerServer(): Unit = {
    system = "peer-server"
    client = AsyncDatomic.clientPeerServer("myaccesskey", "mysecret", "localhost:8998")

    // Using the db associated with the Peer Server connection
    conn = waitFor(client.connect("hello")).toOption.get
  }

  class AsyncSetup extends SchemaAndData with Scope {

    val isDevLocal = system == "dev-local"

    if (isDevLocal) {
      // Re-create db
      waitFor(client.deleteDatabase("hello"))
      waitFor(client.deleteDatabase("world"))
      waitFor(client.createDatabase("hello"))
      conn = waitFor(client.connect("hello")).toOption.get
      waitFor(conn.transact(schema(false))).toOption.get
      txReport = waitFor(conn.transact(data)).toOption.get
    } else {
      // Install schema if necessary
      if (waitFor(AsyncDatomic.q(
        "[:find ?e :where [?e :db/ident :movie/title]]",
        conn.db
      )).head.toOption.get.toString == "[]") {
        println("Installing Peer Server hello db schema...")
        waitFor(conn.transact(schema(true))).toOption.get
      }

      // Retract current data
      waitFor(AsyncDatomic.q("[:find ?e :where [?e :movie/title _]]", conn.db))
        .head.toOption.get
        .asInstanceOf[jList[_]].forEach { l =>
        val eid: Any = l.asInstanceOf[jList[_]].get(0)
        waitFor(conn.transact(list(list(":db/retractEntity", eid)))).toOption.get
      }

      txReport = waitFor(conn.transact(data)).toOption.get
    }

    // Databases before and after last tx (after == current)
    lazy val dbBefore = txReport.dbBefore
    lazy val dbAfter  = txReport.dbAfter

    // Get t before and after last tx
    lazy val tBefore = dbBefore.basisT
    lazy val tAfter  = dbAfter.basisT

    lazy val txIdBefore = Peer.toTx(tBefore).asInstanceOf[Long]
    lazy val txIdAfter  = Peer.toTx(tAfter).asInstanceOf[Long]

    lazy val txData = txReport.txData.toScala(List)
    lazy val txInst = txData.head.v.asInstanceOf[Date]
    lazy val eid    = txData.last.e

    // Entity ids of the three films
    lazy val List(e1, e2, e3) = txData.map(_.e).distinct.drop(1).sorted

    // Ids of the three attributes
    val List(a1, a2, a3) = if (system == "dev-local")
      List(73, 74, 75) else List(72, 73, 74)

    def films(db: AsyncDb): Seq[String] = {
      val lazyList = waitFor(AsyncDatomic.q(filmQuery, db))
      if (lazyList.nonEmpty) {
        lazyList.head.toOption.get.iterator().asScala.toList
          .map(row => row.asInstanceOf[PersistentVector].asScala.toList.head.toString)
          .sorted
      } else Nil
    }
  }
}