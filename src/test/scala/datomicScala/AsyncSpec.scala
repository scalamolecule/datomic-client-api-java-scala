package datomicScala

import java.util.{Date, List => jList}
import clojure.lang.PersistentVector
import datomic.Peer
import datomic.Util.list
import datomicScala.client.api.async._
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import org.specs2.specification.core.{Fragments, Text}
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._


trait AsyncSpec extends Specification with SchemaAndData {

  var system  : String          = "Not set yet. Can be: dev-local / peer-server / cloud"
  var client  : AsyncClient     = null // set in setup
  var conn    : AsyncConnection = null // set in setup
  var txReport: AsyncTxReport   = null // set in setup

  // todo: awaiting to find a way to invoke data as maps against Peer Server
    override def map(fs: => Fragments): Fragments =
      step(setupDevLocal()) ^
        fs.mapDescription(d => Text(s"$system: " + d.show))
//        fs.mapDescription(d => Text(s"$system: " + d.show)) ^
//        step(setupPeerServer()) ^
//        fs.mapDescription(d => Text(s"$system: " + d.show))


  def setupDevLocal(): Unit = {
    system = "dev-local"
    client = AsyncDatomic.clientForDevLocal("free")

    // Re-create db
    client.deleteDatabase("hello").realize
    client.deleteDatabase("world").realize
    client.createDatabase("hello").realize
    conn = client.connect("hello")
    conn.transact(schema(false)).realize

    txReport = conn.transact(data).realize
  }


  def setupPeerServer(): Unit = {
    system = "peer-server"
    client = AsyncDatomic.clientForPeerServer("myaccesskey", "mysecret", "localhost:8998")

    // Using the db associated with the Peer Server connection
    conn = client.connect("hello")

    // Install schema if necessary
    if (AsyncDatomic.q(
      "[:find ?e :where [?e :db/ident :movie/title]]",
      conn.db
    ).realize.toString == "[]") {
      println("Installing Peer Server hello db schema...")
      conn.transact(schema(true)).realize
    }

    // Retract current data
    AsyncDatomic.q("[:find ?e :where [?e :movie/title _]]", conn.db).realize
      .asInstanceOf[jList[_]].forEach { l =>
      val eid: Any = l.asInstanceOf[jList[_]].get(0)
      conn.transact(list(list(":db/retractEntity", eid))).realize
    }

    txReport = conn.transact(data).realize
  }

  class AsyncSetup extends SchemaAndData with Scope {

    lazy val isDevLocal = system == "dev-local"

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
      List(73, 74, 75) else List(63, 64, 65)

    def films(db: AsyncDb): Seq[String] = {
      val res = AsyncDatomic.q(filmQuery, db).realize
      if (res != null) {
        res.iterator().asScala.toList
          .map(row => row.asInstanceOf[PersistentVector].asScala.toList.head.toString)
          .sorted
      } else Nil
    }
  }
}