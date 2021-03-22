package datomicJava.client.api.sync

import java.io.{Reader, StringReader}
import java.lang.{Iterable => jIterable}
import java.util.{List => jList, Map => jMap}
import datomic.Util
import datomic.Util._
import datomicClient._
import datomicClient.anomaly.AnomalyWrapper
import datomicJava.client.api.{Datom, Helper}
import javafx.util.Pair


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
      TxReport(Invoke.transact(datomicConn, stmts).asInstanceOf[jMap[_, _]])
  }

  def transact(stmtsReader: Reader): TxReport =
    transact(readAll(stmtsReader).get(0).asInstanceOf[jList[_]])

  def transact(edn: String): TxReport =
    transact(readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]])


  def txRange(
    timePointStart: Any, // Int | Long | java.util.Date
    timePointEnd: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): jIterable[Pair[Long, jIterable[Datom]]] = {
    val rawTxs0 = {
      val startOpt: Option[Any] = if (timePointStart == 0) None else Some(timePointStart)
      val endOpt  : Option[Any] = if (timePointEnd == 0) None else Some(timePointEnd)
      Invoke.txRange(datomicConn, startOpt, endOpt, timeout, offset, limit)
    }
    Helper.nestedTxsIterable(isDevLocal, rawTxs0)
  }

  def txRange(timePointStart: Any, timePointEnd: Any, limit: Int)
  : jIterable[Pair[Long, jIterable[Datom]]] =
    txRange(timePointStart, timePointEnd, 0, 0, limit)

  def txRange(timePointStart: Any, timePointEnd: Any)
  : jIterable[Pair[Long, jIterable[Datom]]] =
    txRange(timePointStart, timePointEnd, 0, 0, 1000)

  def txRange(limit: Int)
  : jIterable[Pair[Long, jIterable[Datom]]] =
    txRange(0, 0, 0, 0, limit)

  def txRange()
  : jIterable[Pair[Long, jIterable[Datom]]] =
    txRange(0, 0, 0, 0, 1000)


  def txRangeArray(
    timePointStart: Any, // Int | Long | java.util.Date
    timePointEnd: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): Array[Pair[Long, Array[Datom]]] = {
    val rawTxs0 = {
      val startOpt: Option[Any] = if (timePointStart == 0) None else Some(timePointStart)
      val endOpt  : Option[Any] = if (timePointEnd == 0) None else Some(timePointEnd)
      Invoke.txRange(datomicConn, startOpt, endOpt, timeout, offset, limit)
    }
    Helper.nestedTxsArray(isDevLocal, rawTxs0)
  }

  def txRangeArray(timePointStart: Any, timePointEnd: Any, limit: Int)
  : Array[Pair[Long, Array[Datom]]] =
    txRangeArray(timePointStart, timePointEnd, 0, 0, limit)

  def txRangeArray(timePointStart: Any, timePointEnd: Any)
  : Array[Pair[Long, Array[Datom]]] =
    txRangeArray(timePointStart, timePointEnd, 0, 0, 1000)

  def txRangeArray(limit: Int)
  : Array[Pair[Long, Array[Datom]]] =
    txRangeArray(0, 0, 0, 0, limit)

  def txRangeArray()
  : Array[Pair[Long, Array[Datom]]] =
    txRangeArray(0, 0, 0, 0, 1000)


  // Convenience method for single invocation from connection
  def widh(stmts: jList[_]): Db = {
    val txReport = Invoke.`with`(withDb, stmts).asInstanceOf[jMap[_, _]]
    val dbAfter  = txReport.get(read(":db-after"))
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
