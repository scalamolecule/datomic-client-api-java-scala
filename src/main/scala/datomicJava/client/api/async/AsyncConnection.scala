package datomicJava.client.api.async

import java.util.{List => jList, Map => jMap}
import datomic.Util._
import datomicClojure.{ErrorMsg, Invoke, InvokeAsync}
import datomicJava.Helper
import datomicJava.client.api.Datom


case class AsyncConnection(connChannel: Channel[AnyRef]) {

  lazy private val isDevLocal = db.datomicDb.isInstanceOf[datomic.core.db.Db]


  def db: AsyncDb = {
    AsyncDb(
      InvokeAsync.db(connChannel.realize)
    )
  }


  def sync(t: Long): Channel[AsyncDb] = Channel[AsyncDb](
    AsyncDb(
      Channel[datomic.core.db.Db](

        InvokeAsync.sync(connChannel.realize, t)
      ).realize
    )
  )


  def transact(stmts: jList[_]): Channel[AsyncTxReport] = {
    if (stmts.isEmpty)
      throw new IllegalArgumentException(ErrorMsg.transact)

    Channel[AsyncTxReport](
      AsyncTxReport(
        Channel[jMap[_, _]](
          InvokeAsync.transact(connChannel.realize, stmts)
        ).realize
      )
    )
  }


  def txRange(
    start: Long,
    end: Long,
    timeout: Int,
    offset: Int,
    limit: Int
  ): Channel[Array[(Long, Array[Datom])]] = {
    val startOpt: Option[Long] = if (start == 0) None else Some(start)
    val endOpt  : Option[Long] = if (end == 0) None else Some(end)
    val rawTxs0                = Channel[AnyRef](
      Invoke.txRange(connChannel.realize, startOpt, endOpt, timeout, offset, limit)
    ).realize
    Channel[Array[(Long, Array[Datom])]](
      Helper.nestedTxsIterable(isDevLocal, rawTxs0)
    )
  }
  def txRange(): Channel[Array[(Long, Array[Datom])]] = txRange(0, 0, 0, 0, 1000)


  def txRangeArray(
    start: Long,
    end: Long,
    timeout: Int,
    offset: Int,
    limit: Int
  ): Channel[Array[(Long, Array[Datom])]] = {
    val startOpt: Option[Long] = if (start == 0) None else Some(start)
    val endOpt  : Option[Long] = if (end == 0) None else Some(end)
    val rawTxs0                = Channel[AnyRef](
      Invoke.txRange(connChannel.realize, startOpt, endOpt, timeout, offset, limit)
    ).realize
    Channel[Array[(Long, Array[Datom])]](
      Helper.nestedTxsArray(isDevLocal, rawTxs0)
    )
  }
  def txRangeArray(): Channel[Array[(Long, Array[Datom])]] = txRangeArray(0, 0, 0, 0, 1000)


  // Convenience method for single invocation from connection
  def widh(stmts: jList[_]): AsyncDb = {
    val txReport = Channel[AnyRef](
      InvokeAsync.`with`(withDb.realize, stmts)
    ).realize.asInstanceOf[jMap[_, _]]
    val dbAfter  = txReport.get(read(":db-after"))
    AsyncDb(dbAfter.asInstanceOf[AnyRef])
  }

  def withDb: Channel[AnyRef] = Channel[AnyRef](
    // Special db value for `with` (or `widh`)
    InvokeAsync.withDb(connChannel.realize)
  )
}
