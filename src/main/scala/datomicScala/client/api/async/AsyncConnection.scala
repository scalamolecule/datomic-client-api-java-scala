package datomicScala.client.api.async

import java.util.{List => jList, Map => jMap}
import datomic.Util._
import datomicClojure.{ErrorMsg, Invoke, InvokeAsync}
import datomicScala.client.api.Datom
import datomicScala.{CognitectAnomaly, Helper}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class AsyncConnection(datomicConn: AnyRef) {

  lazy private val isDevLocal = db.datomicDb.isInstanceOf[datomic.core.db.Db]


  def db: AsyncDb = AsyncDb(InvokeAsync.db(datomicConn))


  def sync(t: Long): AsyncDb = AsyncDb(
    Channel[datomic.core.db.Db](
      InvokeAsync.sync(datomicConn, t)
    ).lazyList.head.toOption.get // Assuming no anomalies
  )


  def transact(stmts: jList[_]): Future[Either[CognitectAnomaly, AsyncTxReport]] = {
    if (stmts.isEmpty)
      throw new IllegalArgumentException(ErrorMsg.transact)
    Future {
      Channel[jMap[_, _]](
        InvokeAsync.transact(datomicConn, stmts)
      ).lazyList.head match {
        case Right(txReport) => Channel[AsyncTxReport](AsyncTxReport(txReport)).lazyList.head
        case Left(anomaly)   => Left(anomaly)
      }
    }
  }

  def txRange(
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Future[Either[CognitectAnomaly, Iterable[(Long, Iterable[Datom])]]] = Future {
    Channel[AnyRef](
      Invoke.txRange(datomicConn, start, end, timeout, offset, limit)
    ).lazyList.head match {
      case Right(rawTxs0) =>
        Channel[Iterable[(Long, Iterable[Datom])]](
          Helper.nestedTxsIterable(isDevLocal, rawTxs0)
        ).lazyList.head
      case Left(anomaly)  => Left(anomaly)
    }
  }

  def txRangeArray(
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Future[Either[CognitectAnomaly, Array[(Long, Array[Datom])]]] = Future {
    Channel[AnyRef](
      Invoke.txRange(datomicConn, start, end, timeout, offset, limit)
    ).lazyList.head match {
      case Right(rawTxs0) =>
        Channel[Array[(Long, Array[Datom])]](
          Helper.nestedTxsArray(isDevLocal, rawTxs0)
        ).lazyList.head
      case Left(anomaly)  => Left(anomaly)
    }
  }


  // Convenience method for single invocation from connection
  def widh(stmts: jList[_]): Future[Either[CognitectAnomaly, AsyncDb]] = {
    withDb.map {
      case Right(wdb) =>
        Channel[AnyRef](
          InvokeAsync.`with`(wdb, stmts)
        ).lazyList.head match {
          case Right(txMap) =>
            val dbAfter = txMap.asInstanceOf[jMap[_, _]].get(read(":db-after"))
            Right(AsyncDb(dbAfter.asInstanceOf[AnyRef]))

          case Left(anomaly) => Left(anomaly)
        }

      case Left(anomaly) => Left(anomaly)
    }
  }

  def withDb: Future[Either[CognitectAnomaly, AnyRef]] = Future {
    Channel[AnyRef](
      // Special db value for `with` (or `widh`)
      InvokeAsync.withDb(datomicConn)
    ).lazyList.head
  }

}
