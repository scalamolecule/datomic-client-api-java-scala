package datomicScala.client.api.async

import java.util.stream.{Stream => jStream}
import java.util.{Date, List => jList, Map => jMap}
import clojure.lang.{ASeq, LazySeq}
import datomic.Util._
import datomicClojure.{ErrorMsg, InvokeAsync, Lookup}
import datomicScala.client.api.{Datom, DbStats}
import datomicScala.{CognitectAnomaly, Helper}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class AsyncDb(datomicDb: AnyRef) extends Lookup(datomicDb) {

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

  def since(t: Long): AsyncDb = AsyncDb(InvokeAsync.since(datomicDb, t))
  def since(d: Date): AsyncDb = AsyncDb(InvokeAsync.since(datomicDb, d))


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

  // Convenience method to pass with-modified Db from `conn.withDb`
  def `with`(withDbFut: Future[Either[CognitectAnomaly, AnyRef]], stmts: jList[_])
  : Future[Either[CognitectAnomaly, AsyncTxReport]] = {
    withDbFut.flatMap {
      case Right(withDb) => `with`(withDb, stmts)
      case Left(anomaly) => Future(Left(anomaly))
    }
  }

  // Convenience method to pass with-modified Db
  def `with`(db: AsyncDb, stmts: jList[_])
  : Future[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(db.datomicDb, stmts)

  // Convenience method to pass with-modified Db from TxReport
  def `with`(txReport: AsyncTxReport, stmts: jList[_])
  : Future[Either[CognitectAnomaly, AsyncTxReport]] =
    `with`(txReport.dbAfter.datomicDb, stmts)


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
  def datoms(index: String, components: jList[_])
  : Future[Either[CognitectAnomaly, jStream[Datom]]] = Future {
    Channel[Any](
      InvokeAsync.datoms(datomicDb, index, components)
    ).lazyList.head match {
      case Right(datoms) =>
        Channel[jStream[Datom]](Helper.streamOfDatoms(datoms)).lazyList.head
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
    ).lazyList.head match {
      case Right(datoms) =>
        Channel[jStream[Datom]](Helper.streamOfDatoms(datoms)).lazyList.head
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
    ).lazyList.head
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
    if (!Seq(":avet", ":aevt").contains(index))
      throw new IllegalArgumentException(ErrorMsg.indexPull)
    Future(
      Channel[Any](
        InvokeAsync.indexPull(
          datomicDb, index, selector, start, reverse, timeout, offset, limit
        )
      ).lazyList.head match {
        case Right(indexPull) =>
          Channel[jStream[_]](
            indexPull.asInstanceOf[LazySeq].stream()
          ).lazyList.head

        case Left(anomaly) => Left(anomaly)
      }
    )
  }
}