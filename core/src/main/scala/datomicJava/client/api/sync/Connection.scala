package datomicJava.client.api.sync

import java.lang.{Iterable => jIterable}
import java.util.{List => jList, Map => jMap}
import datomic.Util
import datomic.Util._
import datomicClojure.Invoke
import datomicJava.client.api.Datom
import datomicJava.{AnomalyWrapper, Helper}
import javafx.util.Pair


case class Connection(datomicConn: AnyRef) extends AnomalyWrapper {

  lazy private val isDevLocal = db.datomicDb.isInstanceOf[clojure.lang.IPersistentMap]

  def db: Db = catchAnomaly {
    Db(Invoke.db(datomicConn))
  }


  def sync(t: Long): Db = catchAnomaly {
    Db(Invoke.sync(datomicConn, t))
  }


  def transact(stmts: jList[_]): TxReport = catchAnomaly {
    if (stmts.isEmpty)
      TxReport(Util.map())
    else
      TxReport(Invoke.transact(datomicConn, stmts).asInstanceOf[jMap[_, _]])
  }


  def txRange(
    timePointStart: Any, // Int | Long | java.util.Date
    timePointEnd: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): jIterable[Pair[Long, jIterable[Datom]]] = {
    val rawTxs0 = catchAnomaly {
      val startOpt: Option[Any] = if (timePointStart == 0) None else Some(timePointStart)
      val endOpt  : Option[Any] = if (timePointEnd == 0) None else Some(timePointEnd)
      Invoke.txRange(datomicConn, startOpt, endOpt, timeout, offset, limit)
    }
    Helper.nestedTxsIterable(isDevLocal, rawTxs0)
  }

  def txRange()
  : jIterable[Pair[Long, jIterable[Datom]]] =
    txRange(0, 0, 0, 0, -1)

  def txRange(limit: Int)
  : jIterable[Pair[Long, jIterable[Datom]]] =
    txRange(0, 0, 0, 0, limit)

  def txRange(timePointStart: Any, timePointEnd: Any)
  : jIterable[Pair[Long, jIterable[Datom]]] =
    txRange(timePointStart, timePointEnd, 0, 0, -1)

  def txRange(timePointStart: Any, timePointEnd: Any, limit: Int)
  : jIterable[Pair[Long, jIterable[Datom]]] =
    txRange(timePointStart, timePointEnd, 0, 0, limit)


  def txRangeArray(
    timePointStart: Any, // Int | Long | java.util.Date
    timePointEnd: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): Array[Pair[Long, Array[Datom]]] = {
    val rawTxs0 = catchAnomaly {
      val startOpt: Option[Any] = if (timePointStart == 0) None else Some(timePointStart)
      val endOpt  : Option[Any] = if (timePointEnd == 0) None else Some(timePointEnd)
      Invoke.txRange(datomicConn, startOpt, endOpt, timeout, offset, limit)
    }
    Helper.nestedTxsArray(isDevLocal, rawTxs0)
  }

  def txRangeArray()
  : Array[Pair[Long, Array[Datom]]] =
    txRangeArray(0, 0, 0, 0, -1)

  def txRangeArray(limit: Int)
  : Array[Pair[Long, Array[Datom]]] =
    txRangeArray(0, 0, 0, 0, limit)

  def txRangeArray(timePointStart: Any, timePointEnd: Any)
  : Array[Pair[Long, Array[Datom]]] =
    txRangeArray(timePointStart, timePointEnd, 0, 0, -1)

  def txRangeArray(timePointStart: Any, timePointEnd: Any, limit: Int)
  : Array[Pair[Long, Array[Datom]]] =
    txRangeArray(timePointStart, timePointEnd, 0, 0, limit)


  // Convenience method for single invocation from connection
  def widh(stmts: jList[_]): Db = catchAnomaly {
    val txReport = Invoke.`with`(withDb, stmts).asInstanceOf[jMap[_, _]]
    val dbAfter  = txReport.get(read(":db-after"))
    Db(dbAfter.asInstanceOf[AnyRef])
  }

  def withDb: AnyRef = catchAnomaly {
    // Special db value for `with` (or `widh`)
    Invoke.withDb(datomicConn)
  }
}
