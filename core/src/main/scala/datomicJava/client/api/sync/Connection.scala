package datomicJava.client.api.sync

import java.lang.{Iterable => jIterable}
import java.util.{List => jList, Map => jMap}
import datomic.Util._
import datomicClojure.{ErrorMsg, Invoke}
import datomicJava.{AnomalyWrapper, Helper}
import datomicJava.client.api.Datom
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
      throw new IllegalArgumentException(ErrorMsg.transact)
    TxReport(Invoke.transact(datomicConn, stmts).asInstanceOf[jMap[_, _]])
  }


  def txRange(
    start: Long,
    end: Long,
    timeout: Int,
    offset: Int,
    limit: Int
  ): jIterable[Pair[Long, jIterable[Datom]]] = {
    val rawTxs0 = catchAnomaly {
      val startOpt: Option[Long] = if (start == 0) None else Some(start)
      val endOpt  : Option[Long] = if (end == 0) None else Some(end)
      Invoke.txRange(datomicConn, startOpt, endOpt, timeout, offset, limit)
    }
    Helper.nestedTxsIterable(isDevLocal, rawTxs0)
  }
  def txRange(): jIterable[Pair[Long, jIterable[Datom]]] = txRange(0, 0, 0, 0, 1000)
  def txRange(limit: Int): jIterable[Pair[Long, jIterable[Datom]]] = txRange(0, 0, 0, 0, limit)


  def txRangeArray(
    start: Long,
    end: Long,
    timeout: Int,
    offset: Int,
    limit: Int
  ): Array[Pair[Long, Array[Datom]]] = {
    val rawTxs0 = catchAnomaly {
      val startOpt: Option[Long] = if (start == 0) None else Some(start)
      val endOpt  : Option[Long] = if (end == 0) None else Some(end)
      Invoke.txRange(datomicConn, startOpt, endOpt, timeout, offset, limit)
    }
    Helper.nestedTxsArray(isDevLocal, rawTxs0)
  }
  def txRangeArray(): Array[Pair[Long, Array[Datom]]] = txRangeArray(0, 0, 0, 0, 1000)
  def txRangeArray(limit: Int): Array[Pair[Long, Array[Datom]]] = txRangeArray(0, 0, 0, 0, limit)


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
