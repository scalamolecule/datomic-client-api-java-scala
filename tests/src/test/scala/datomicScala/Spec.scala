package datomicScala

import java.util.{Date, List => jList}
import clojure.lang.PersistentVector
import datomic.Peer
import datomic.Util.list
import datomicScala.client.api.sync._
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import org.specs2.specification.core.{Fragments, Text}
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._

trait Spec extends Specification with SchemaAndData {

  var system  : String     = "Not set yet. Can be: dev-local / peer-server / cloud"
  var client  : Client     = null // set in setup
  var conn    : Connection = null // set in setup
  var txReport: TxReport   = null // set in setup

  override def map(fs: => Fragments): Fragments =
    step(setupDevLocal()) ^
      fs.mapDescription(d => Text(s"$system: " + d.show)) ^
      step(setupPeerServer()) ^
      fs.mapDescription(d => Text(s"$system: " + d.show))

  def setupDevLocal(): Unit = {
    system = "dev-local"
    println(1)
    client = Datomic.clientDevLocal("Hello system name")
  }

  def setupPeerServer(): Unit = {
    system = "peer-server"
    client = Datomic.clientPeerServer("myaccesskey", "mysecret", "localhost:8998")
    conn = try {
      client.connect("hello")
    } catch {
      case e: CognitectAnomaly =>
        println(e)
        println(e.msg)
        throw e
      case t: Throwable        => throw t
    }
  }

  def resetDevLocalDb(): Unit = {
    // Re-create db
    client.deleteDatabase("hello")
    client.deleteDatabase("world")
    client.createDatabase("hello")
    conn = client.connect("hello")
    conn.transact(schema(false))
    txReport = conn.transact(data)
  }

  def resetPeerServerDb(): Unit = {
    // Install schema if necessary
    if (Datomic.q(
      "[:find ?e :where [?e :db/ident :movie/title]]",
      conn.db
    ).toString == "[]") {
      println("Installing Peer Server hello db schema...")
      conn.transact(schema(true))
    }

    // Retract current data
    Datomic.q("[:find ?e :where [?e :movie/title _]]", conn.db)
      .asInstanceOf[jList[_]].forEach { l =>
      val eid: Any = l.asInstanceOf[jList[_]].get(0)
      conn.transact(list(list(":db/retractEntity", eid)))
    }

    txReport = conn.transact(data)
  }


  class Setup extends SchemaAndData with Scope {

    val isDevLocal = system == "dev-local"

    if (isDevLocal) resetDevLocalDb() else resetPeerServerDb()

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

    def films(db: Db): Seq[String] = Datomic.q(filmQuery, db)
      .asInstanceOf[PersistentVector].asScala.toList
      .map(row => row.asInstanceOf[PersistentVector].asScala.head.toString)
      .sorted
  }
}