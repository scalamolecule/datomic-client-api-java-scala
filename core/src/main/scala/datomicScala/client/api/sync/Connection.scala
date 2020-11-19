package datomicScala.client.api.sync

import java.util.{List => jList, Map => jMap}
import datomic.Util
import datomic.Util._
import datomicClojure.{ErrorMsg, Invoke}
import datomicScala.client.api.Datom
import datomicScala.{AnomalyWrapper, Helper}


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
      TxReport(
        Invoke.transact(datomicConn, stmts).asInstanceOf[jMap[_, _]]
      )
  }


  def txRange(
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Iterable[(Long, Iterable[Datom])] = {
    val rawTxs0 = catchAnomaly {
      Invoke.txRange(datomicConn, start, end, timeout, offset, limit)
    }
    Helper.nestedTxsIterable(isDevLocal, rawTxs0)
  }


  // Populated nested Arrays of txs and datoms
  def txRangeArray(
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Array[(Long, Array[Datom])] = {
    val rawTxs0 = catchAnomaly {
      Invoke.txRange(datomicConn, start, end, timeout, offset, limit)
    }
    Helper.nestedTxsArray(isDevLocal, rawTxs0)
  }


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
