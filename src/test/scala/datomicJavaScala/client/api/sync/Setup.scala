package datomicJavaScala.client.api.sync

import java.util.Date
import clojure.lang.PersistentVector
import datomic.Peer
import datomicJavaScala.SchemaAndData
import datomicJavaScala.client.api._
import org.specs2.specification.Scope
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._

class Setup extends SchemaAndData with Scope {
  lazy val system  : String     = ClientProvider.system
  lazy val client  : Client     = ClientProvider.client
  lazy val conn    : Connection = ClientProvider.conn
  lazy val txReport: TxReport   = ClientProvider.txReport

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

  def films(db: Db): Seq[String] = Datomic.q(filmQuery, db)
    .asInstanceOf[PersistentVector].asScala.toList
    .map(row => row.asInstanceOf[PersistentVector].asScala.head.toString)
    .sorted
}
