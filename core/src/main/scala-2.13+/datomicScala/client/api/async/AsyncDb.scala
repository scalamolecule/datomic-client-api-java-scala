package datomicScala.client.api.async

import java.io.{Reader, StringReader}
import java.util.stream.{Stream => jStream}
import java.util.{Collections, Date, List => jList, Map => jMap, Collection => jCollection}
import clojure.lang.LazySeq
import datomic.Util.readAll
import datomicClient._
import datomicClient.anomaly.CognitectAnomaly
import datomicScala.client.api.sync.{Db, TxReport}
import datomicScala.client.api.{Datom, DbStats, Helper}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class AsyncDb(
  datomicDb: AnyRef,
  sinceTimePoint: Option[(Long, Long, Date)] = None
) extends DbLookup(datomicDb, sinceTimePoint) {

  def dbStats: Future[Either[CognitectAnomaly, DbStats]] = Future {
    Channel[jMap[_, _]](
      InvokeAsync.dbStats(datomicDb)
    ).lazyList.head match {
      case Right(dbStats) =>
        Channel[DbStats](Helper.dbStats(isDevLocal, dbStats)).lazyList.head
      case Left(anomaly)  => Left(anomaly)
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
  : Future[Either[CognitectAnomaly, AsyncTxReport]] = {
    Future(
      Channel[AnyRef](
        InvokeAsync.`with`(withDb, stmts)
      ).lazyList.head match {
        case Right(withDb) =>
          Channel[AsyncTxReport](
            AsyncTxReport(
              withDb.asInstanceOf[jMap[_, _]]
            )
          ).lazyList.head
        case Left(anomaly) => Left(anomaly)
      }
    )
  }

  def `with`(withDb: AnyRef, stmtsReader: Reader)
  : Future[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(withDb, readAll(stmtsReader).get(0).asInstanceOf[jList[_]])

  def `with`(withDb: AnyRef, edn: String)
  : Future[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(withDb, readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]])


  // Convenience method to pass with-modified Db from `conn.withDb`
  def `with`(withDbFut: Future[Either[CognitectAnomaly, AnyRef]], stmts: jList[_])
  : Future[Either[CognitectAnomaly, AsyncTxReport]] = {
    withDbFut.flatMap {
      case Right(withDb) => `with`(withDb, stmts)
      case Left(anomaly) => Future(Left(anomaly))
    }
  }

  def `with`(
    withDbFut: Future[Either[CognitectAnomaly, AnyRef]],
    stmtsReader: Reader
  ): Future[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(withDbFut, readAll(stmtsReader).get(0).asInstanceOf[jList[_]])

  def `with`(
    withDbFut: Future[Either[CognitectAnomaly, AnyRef]],
    edn: String
  ): Future[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(withDbFut, readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]])


  // Convenience method to pass with-modified Db
  def `with`(db: AsyncDb, stmts: jList[_])
  : Future[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(db.datomicDb, stmts)

  def `with`(db: AsyncDb, stmtsReader: Reader)
  : Future[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(db.datomicDb, readAll(stmtsReader).get(0).asInstanceOf[jList[_]])

  def `with`(db: AsyncDb, edn: String)
  : Future[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(db.datomicDb, readAll(new StringReader(edn)).get(0).asInstanceOf[jList[_]])


  // Convenience method to pass with-modified Db from TxReport
  def `with`(txReport: AsyncTxReport, stmts: jList[_])
  : Future[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(txReport.dbAfter.datomicDb, stmts)

  def `with`(txReport: AsyncTxReport, stmtsReader: Reader)
  : Future[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(
      txReport.dbAfter.datomicDb,
      readAll(stmtsReader).get(0).asInstanceOf[jList[_]]
    )

  def `with`(txReport: AsyncTxReport, edn: String)
  : Future[Either[CognitectAnomaly, AsyncTxReport]] =
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
   * @return Future[Either[CognitectAnomaly, jStream[Datom]]]
   */
  def datoms(
    index: String,
    components: jList[_],
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Future[Either[CognitectAnomaly, jStream[Datom]]] = Future {
    Channel[Any](
      InvokeAsync.datoms(datomicDb, index, components, timeout, offset, limit)
    ).lazyList.headOption.fold(
      Channel[jStream[Datom]](jStream.empty()).lazyList.head
    ) {
      case Right(datoms) => Channel[jStream[Datom]](
        Helper.streamOfDatoms(datoms)
      ).lazyList.head

      case Left(anomaly) => Left(anomaly)
    }
  }


  def indexRange(
    attrId: String,
    start: Option[Any] = None,
    end: Option[Any] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Future[Either[CognitectAnomaly, jStream[Datom]]] = Future {
    Channel[Any](
      InvokeAsync.indexRange(
        datomicDb, attrId, start, end, timeout, offset, limit
      )
    ).lazyList.headOption.fold(
      Channel[jStream[Datom]](jStream.empty()).lazyList.head
    ) {
      case Right(datoms) => Channel[jStream[Datom]](
        Helper.streamOfDatoms(datoms)
      ).lazyList.head

      case Left(anomaly) => Left(anomaly)
    }
  }


  // Pull --------------------------------------

  def pull(
    selector: String,
    eid: Any,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Future[Either[CognitectAnomaly, jMap[_, _]]] = Future {
    Channel[jMap[_, _]](
      InvokeAsync.pull(
        datomicDb, selector, eid, timeout, offset, limit
      )
    ).lazyList.headOption.getOrElse(
      Channel[jMap[_, _]](Collections.EMPTY_MAP).lazyList.head
    )
  }


  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean = false,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Future[Either[CognitectAnomaly, jStream[_]]] = {
    Future(
      Channel[Any](
        InvokeAsync.indexPull(
          datomicDb, index, selector, start, reverse, timeout, offset, limit
        )
      ).lazyList.headOption.fold(
        Channel[jStream[_]](jStream.empty()).lazyList.head
      ) {
        case Right(indexPull) =>
          Channel[jStream[_]](
            indexPull.asInstanceOf[jCollection[_]].stream()
          ).lazyList.head

        case Left(anomaly) => Left(anomaly)
      }
    )
  }
}