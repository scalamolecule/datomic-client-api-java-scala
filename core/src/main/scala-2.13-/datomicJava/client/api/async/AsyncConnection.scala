package datomicJava.client.api.async

import java.io.{Reader, StringReader}
import java.lang.{Iterable => jIterable}
import java.util.concurrent.CompletableFuture
import java.util.{List => jList, Map => jMap}
import datomic.Util
import datomic.Util._
import datomicClient._
import datomicClient.anomaly.CognitectAnomaly
import datomicJava.client.api.{Datom, Helper, async}
import javafx.util.Pair


case class AsyncConnection(datomicConn: AnyRef) {

  lazy private val isDevLocal = db.datomicDb.isInstanceOf[clojure.lang.IPersistentMap]


  def db: AsyncDb = AsyncDb(InvokeAsync.db(datomicConn))


  def sync(t: Long): AsyncDb = AsyncDb(
    Channel[AnyRef](
      InvokeAsync.sync(datomicConn, t)
      // Assuming no anomalies
    ).chunk.asInstanceOf[Right[_, AnyRef]].right_value
  )


  def transact(stmts: jList[_]): CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] = {
    if (stmts.isEmpty)
      CompletableFuture.supplyAsync(() =>
        Channel[AsyncTxReport](AsyncTxReport(Util.map())).chunk
      )
    else
      CompletableFuture.supplyAsync { () =>
        Channel[jMap[_, _]](
          InvokeAsync.transact(datomicConn, stmts)
        ).chunk match {
          case Right(txReport) => Channel[AsyncTxReport](AsyncTxReport(txReport)).chunk
          case Left(anomaly)   => async.Left(anomaly)
        }
      }
  }

  def transact(stmtsReader: Reader): CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    transact(readAll(stmtsReader).get(0).asInstanceOf[jList[_]])

  def transact(edn: String): CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    transact(readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]])


  def txRange(
    timePointStart: Any, // Int | Long | java.util.Date
    timePointEnd: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): CompletableFuture[Either[CognitectAnomaly, jIterable[Pair[Long, jIterable[Datom]]]]] =
    CompletableFuture.supplyAsync { () =>
      val startOpt = if (timePointStart == 0) None else Some(timePointStart)
      val endOpt   = if (timePointEnd == 0) None else Some(timePointEnd)
      Channel[AnyRef](
        Invoke.txRange(datomicConn, startOpt, endOpt, timeout, offset, limit)
      ).chunk match {
        case Right(rawTxs0) =>
          Channel[jIterable[Pair[Long, jIterable[Datom]]]](
            Helper.nestedTxsIterable(isDevLocal, rawTxs0)
          ).chunk
        case Left(anomaly)  => async.Left(anomaly)
      }
    }

  def txRange(timePointStart: Any, timePointEnd: Any, limit: Int)
  : CompletableFuture[Either[CognitectAnomaly, jIterable[Pair[Long, jIterable[Datom]]]]] =
    txRange(timePointStart, timePointEnd, 0, 0, limit)

  def txRange(timePointStart: Any, timePointEnd: Any)
  : CompletableFuture[Either[CognitectAnomaly, jIterable[Pair[Long, jIterable[Datom]]]]] =
    txRange(timePointStart, timePointEnd, 0, 0, 1000)

  def txRange(limit: Int)
  : CompletableFuture[Either[CognitectAnomaly, jIterable[Pair[Long, jIterable[Datom]]]]] =
    txRange(0, 0, 0, 0, limit)

  def txRange()
  : CompletableFuture[Either[CognitectAnomaly, jIterable[Pair[Long, jIterable[Datom]]]]] =
    txRange(0, 0, 0, 0, 1000)


  def txRangeArray(
    timePointStart: Any, // Int | Long | java.util.Date
    timePointEnd: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): CompletableFuture[Either[CognitectAnomaly, Array[Pair[Long, Array[Datom]]]]] = {
    CompletableFuture.supplyAsync { () =>
      val startOpt = if (timePointStart == 0) None else Some(timePointStart)
      val endOpt   = if (timePointEnd == 0) None else Some(timePointEnd)
      Channel[AnyRef](
        Invoke.txRange(datomicConn, startOpt, endOpt, timeout, offset, limit)
      ).chunk match {
        case Right(rawTxs0) =>
          Channel[Array[Pair[Long, Array[Datom]]]](
            Helper.nestedTxsArray(isDevLocal, rawTxs0)
          ).chunk
        case Left(anomaly)  => async.Left(anomaly)
      }
    }
  }

  def txRangeArray(timePointStart: Any, timePointEnd: Any, limit: Int
  ): CompletableFuture[Either[CognitectAnomaly, Array[Pair[Long, Array[Datom]]]]] =
    txRangeArray(timePointStart, timePointEnd, 0, 0, limit)

  def txRangeArray(timePointStart: Any, timePointEnd: Any
  ): CompletableFuture[Either[CognitectAnomaly, Array[Pair[Long, Array[Datom]]]]] =
    txRangeArray(timePointStart, timePointEnd, 0, 0, 1000)

  def txRangeArray(limit: Int)
  : CompletableFuture[Either[CognitectAnomaly, Array[Pair[Long, Array[Datom]]]]] =
    txRangeArray(0, 0, 0, 0, limit)

  def txRangeArray()
  : CompletableFuture[Either[CognitectAnomaly, Array[Pair[Long, Array[Datom]]]]] =
    txRangeArray(0, 0, 0, 0, 1000)


  // Convenience method for single invocation from connection
  def widh(stmts: jList[_]): CompletableFuture[Either[CognitectAnomaly, AsyncDb]] = {
    withDb.thenApply {
      case Right(wdb) =>
        Channel[AnyRef](
          InvokeAsync.`with`(wdb, stmts)
        ).chunk match {
          case Right(txMap) =>
            val dbAfter = txMap.asInstanceOf[jMap[_, _]].get(read(":db-after"))
            async.Right(AsyncDb(dbAfter.asInstanceOf[AnyRef]))

          case Left(anomaly) => async.Left(anomaly)
        }

      case Left(anomaly) => async.Left(anomaly)
    }
  }

  def widh(stmtsReader: Reader): CompletableFuture[Either[CognitectAnomaly, AsyncDb]] =
    widh(readAll(stmtsReader).get(0).asInstanceOf[jList[_]])

  def widh(edn: String): CompletableFuture[Either[CognitectAnomaly, AsyncDb]] =
    widh(readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]])


  def withDb: CompletableFuture[Either[CognitectAnomaly, AnyRef]] = {
    // Special db value for `with` (or `widh`)
    CompletableFuture.supplyAsync { () =>
      Channel[AnyRef](
        // Special db value for `with` (or `widh`)
        InvokeAsync.withDb(datomicConn)
      ).chunk
    }
  }
}
