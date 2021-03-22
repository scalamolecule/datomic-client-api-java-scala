package datomicJava.client.api.async

import java.io.{Reader, StringReader}
import java.util.concurrent.CompletableFuture
import java.util.stream.{Stream => jStream}
import java.util.{Date, List => jList, Map => jMap}
import clojure.lang.LazySeq
import datomic.Util.readAll
import datomicClient._
import datomicClient.anomaly.CognitectAnomaly
import datomicJava.client.api.{Datom, DbStats, Helper, async}


case class AsyncDb(
  datomicDb: AnyRef,
  sinceTimePoint: Option[(Long, Long, Date)] = None
) extends DbLookup(datomicDb, sinceTimePoint) {


  def dbStats: CompletableFuture[Either[CognitectAnomaly, DbStats]] = {
    CompletableFuture.supplyAsync { () =>
      Channel[jMap[_, _]](
        InvokeAsync.dbStats(datomicDb)
      ).chunk
    }.thenApply {
      case Right(dbStats: jMap[_, _]) =>
        Channel[DbStats](Helper.dbStats(isDevLocal, dbStats)).chunk
      case Left(anomaly)              => async.Left(anomaly)
    }
  }

  // Time filters --------------------------------------

  def asOf(t: Long): AsyncDb = AsyncDb(InvokeAsync.asOf(datomicDb, t))

  def asOf(d: Date): AsyncDb = AsyncDb(InvokeAsync.asOf(datomicDb, d))


  def since(tOrTx: Long): AsyncDb =
    AsyncDb(InvokeAsync.since(datomicDb, tOrTx), extractSinceTimePoint(tOrTx))

  def since(d: Date): AsyncDb =
    AsyncDb(InvokeAsync.since(datomicDb, d), extractSinceTimePoint(d))


  // Presuming a `withDb` is passed.
  def `with`(withDb: AnyRef, stmts: jList[_])
  : CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] = {
    CompletableFuture.supplyAsync { () =>
      Channel[AnyRef](
        InvokeAsync.`with`(withDb, stmts)
      ).chunk
    }.thenApply {
      case Right(withDb) =>
        Channel[AsyncTxReport](
          AsyncTxReport(
            withDb.asInstanceOf[jMap[_, _]]
          )
        ).chunk
      case Left(anomaly) => async.Left(anomaly)
    }
  }

  def `with`(withDb: AnyRef, stmtsReader: Reader)
  : CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(withDb, readAll(stmtsReader).get(0).asInstanceOf[jList[_]])

  def `with`(withDb: AnyRef, edn: String)
  : CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(withDb, readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]])


  // Convenience method to pass with-modified Db from `conn.withDb`
  def `with`(withDbFut: CompletableFuture[Either[CognitectAnomaly, AnyRef]], stmts: jList[_])
  : CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] = {
    withDbFut.thenApply {
      case Right(withDb) =>
        Right(
          `with`(withDb, stmts)
            .get.asInstanceOf[Right[_, AsyncTxReport]].right_value
        )

      case Left(anomaly) => async.Left(anomaly)
    }
  }

  def `with`(
    withDbFut: CompletableFuture[Either[CognitectAnomaly, AnyRef]],
    stmtsReader: Reader
  ): CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(withDbFut, readAll(stmtsReader).get(0).asInstanceOf[jList[_]])

  def `with`(
    withDbFut: CompletableFuture[Either[CognitectAnomaly, AnyRef]],
    edn: String
  ): CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(withDbFut, readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]])


  // Convenience method to pass with-modified Db
  def `with`(db: AsyncDb, stmts: jList[_])
  : CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(db.datomicDb, stmts)

  def `with`(db: AsyncDb, stmtsReader: Reader)
  : CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(db.datomicDb, readAll(stmtsReader).get(0).asInstanceOf[jList[_]])

  def `with`(db: AsyncDb, edn: String)
  : CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(db.datomicDb, readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]])


  // Convenience method to pass with-modified Db from TxReport
  def `with`(txReport: AsyncTxReport, stmts: jList[_])
  : CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(txReport.dbAfter.datomicDb, stmts)

  def `with`(txReport: AsyncTxReport, stmtsReader: Reader)
  : CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(
      txReport.dbAfter.datomicDb,
      readAll(stmtsReader).get(0).asInstanceOf[jList[_]]
    )

  def `with`(txReport: AsyncTxReport, edn: String)
  : CompletableFuture[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(
      txReport.dbAfter.datomicDb,
      readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]]
    )


  def history: AsyncDb = AsyncDb(
    InvokeAsync.history(datomicDb)
  )


  // Indexes --------------------------------------

  /**
   *
   * @param index      String :eavt, :aevt, :avet, or :vaet
   * @param components Optional vector in the same order as the index
   *                   containing one or more values to further narrow the
   *                   result.
   * @return List[datomicFacade.client.api.Datom] Wrapped Datoms with a unified api
   */
  def datoms(
    index: String,
    components: jList[_],
    timeout: Int,
    offset: Int,
    limit: Int
  ): CompletableFuture[Either[CognitectAnomaly, jStream[Datom]]] = {
    CompletableFuture.supplyAsync { () =>
      Channel[Any](
        InvokeAsync.datoms(datomicDb, index, components, timeout, offset, limit)
      ).chunk
    }.thenApply {
      case Right(datoms) => Channel[jStream[Datom]](
        Helper.streamOfDatoms(datoms)
      ).chunk
      case Left(anomaly) => async.Left(anomaly)
    }
  }

  def datoms(
    index: String,
    components: jList[_],
    timeout: Int
  ): CompletableFuture[Either[CognitectAnomaly, jStream[Datom]]] =
    datoms(index, components, timeout, 0, 1000)

  def datoms(
    index: String,
    components: jList[_]
  ): CompletableFuture[Either[CognitectAnomaly, jStream[Datom]]] =
    datoms(index, components, 0, 0, 1000)


  def indexRange(
    attrId: String,
    start0: Any,
    end0: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): CompletableFuture[Either[CognitectAnomaly, jStream[Datom]]] = {
    val start = Option(start0)
    val end = Option(end0)
    CompletableFuture.supplyAsync { () =>
      Channel[Any](
        InvokeAsync.indexRange(
          datomicDb, attrId, start, end, timeout, offset, limit
        )
      ).chunk
    }.thenApply {
      case Right(datoms) => Channel[jStream[Datom]](
        Helper.streamOfDatoms(datoms)
      ).chunk
      case Left(anomaly) => async.Left(anomaly)
    }
  }

  def indexRange[T](
    attrId: String,
    start: Any,
    end: Any
  ): CompletableFuture[Either[CognitectAnomaly, jStream[Datom]]] =
    indexRange(attrId, start, end, 0, 0, 1000)

  def indexRange[T](attrId: String, start: Long)
  : CompletableFuture[Either[CognitectAnomaly, jStream[Datom]]] =
    indexRange(attrId, start, null, 0, 0, 1000)

  def indexRange[T](attrId: String)
  : CompletableFuture[Either[CognitectAnomaly, jStream[Datom]]] =
    indexRange(attrId, null, null, 0, 0, 1000)


  // Pull --------------------------------------

  def pull(
    selector: String,
    eid: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): CompletableFuture[Either[CognitectAnomaly, jMap[_, _]]] = {
    CompletableFuture.supplyAsync { () =>
      Channel[jMap[_, _]](
        InvokeAsync.pull(datomicDb, selector, eid, timeout, offset, limit)
      ).chunk
    }
  }

  def pull(selector: String, eid: Any, limit: Int)
  : CompletableFuture[Either[CognitectAnomaly, jMap[_, _]]] =
    pull(selector, eid, 0, 0, limit)

  def pull(selector: String, eid: Any)
  : CompletableFuture[Either[CognitectAnomaly, jMap[_, _]]] =
    pull(selector, eid, 0, 0, 1000)


  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean,
    timeout: Int,
    offset: Int,
    limit: Int,
  ): CompletableFuture[Either[CognitectAnomaly, jStream[_]]] = {
    CompletableFuture.supplyAsync { () =>
      Channel[Any](
        InvokeAsync.indexPull(
          datomicDb, index, selector, start, reverse, timeout, offset, limit
        )
      ).chunk
    }.thenApply {
      case Right(indexPull) => Channel[jStream[_]](
        indexPull.asInstanceOf[LazySeq].stream()
      ).chunk
      case Left(anomaly)    => async.Left(anomaly)
    }
  }

  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean,
    limit: Int,
  ): CompletableFuture[Either[CognitectAnomaly, jStream[_]]] = {
    indexPull(index, selector, start, reverse, 0, 0, limit)
  }

  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean
  ): CompletableFuture[Either[CognitectAnomaly, jStream[_]]] = {
    indexPull(index, selector, start, reverse, 0, 0, 1000)
  }

  def indexPull(
    index: String,
    selector: String,
    start: String
  ): CompletableFuture[Either[CognitectAnomaly, jStream[_]]] = {
    indexPull(index, selector, start, false, 0, 0, 1000)
  }
}