package datomicScala.client.api.async

import java.util.{List => jList, Map => jMap}
import datomic.Util._
import datomicClojure.{ErrorMsg, Invoke, InvokeAsync}
import datomicScala.Helper
import datomicScala.client.api.Datom


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
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Channel[Iterable[(Long, Iterable[Datom])]] = {
    val rawTxs0 = Channel[AnyRef](
      Invoke.txRange(connChannel.realize, start, end, timeout, offset, limit)
    ).realize
    Channel[Iterable[(Long, Iterable[Datom])]](
      Helper.nestedTxsIterable(isDevLocal, rawTxs0)
    )
  }

  def txRangeArray(
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Channel[Array[(Long, Array[Datom])]] = {
    val rawTxs0 = Channel[AnyRef](
      Invoke.txRange(connChannel.realize, start, end, timeout, offset, limit)
    ).realize
    Channel[Array[(Long, Array[Datom])]](
      Helper.nestedTxsArray(isDevLocal, rawTxs0)
    )
  }


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
