package datomicScala.client.api.sync

import java.io.{Reader, StringReader}
import java.util.{List => jList, Map => jMap}
import datomic.Util
import datomic.Util._
import datomicClient._
import datomicClient.anomaly.AnomalyWrapper
import datomicScala.client.api.{Datom, Helper}


case class Connection(datomicConn: AnyRef) extends AnomalyWrapper {

  lazy private val isDevLocal = db.datomicDb.isInstanceOf[clojure.lang.IPersistentMap]

  def db: Db = {
    Db(Invoke.db(datomicConn))
  }


  def sync(t: Long): Db = {
    Db(Invoke.sync(datomicConn, t))
  }


  def transact(stmts: jList[_]): TxReport = {
    if (stmts.isEmpty)
      TxReport(Util.map())
    else
      TxReport(
        Invoke.transact(datomicConn, stmts).asInstanceOf[jMap[_, _]]
      )
  }

  def transact(stmtsReader: Reader): TxReport =
    transact(readAll(stmtsReader).get(0).asInstanceOf[jList[_]])

  def transact(edn: String): TxReport =
    transact(readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]])



  def txRange(
    timePointStart: Option[Any] = None, // Int | Long | java.util.Date
    timePointEnd: Option[Any] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Iterable[(Long, Iterable[Datom])] = {
    val rawTxs0 = {
      Invoke.txRange(datomicConn, timePointStart, timePointEnd, timeout, offset, limit)
    }
    Helper.nestedTxsIterable(isDevLocal, rawTxs0)
  }

  // Convenience method to get populated nested Arrays of txs and datoms
  def txRangeArray(
    timePointStart: Option[Any] = None, // Int | Long | java.util.Date
    timePointEnd: Option[Any] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Array[(Long, Array[Datom])] = {
    val rawTxs0 = {
      Invoke.txRange(datomicConn, timePointStart, timePointEnd, timeout, offset, limit)
    }
    Helper.nestedTxsArray(isDevLocal, rawTxs0)
  }


  // Convenience method for single invocation from connection
  def widh(stmts: jList[_]): Db = {
    val rawTxReport = Invoke.`with`(withDb, stmts).asInstanceOf[jMap[_, _]]
    val dbAfter  = rawTxReport.get(read(":db-after"))
    Db(dbAfter.asInstanceOf[AnyRef])
  }

  def widh(stmtsReader: Reader): Db =
    widh(readAll(stmtsReader).get(0).asInstanceOf[jList[_]])

  def widh(edn: String): Db =
    widh(readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]])


  def withDb: AnyRef = {
    // Special db value for `with` (or `widh`)
    Invoke.withDb(datomicConn)
  }
}
