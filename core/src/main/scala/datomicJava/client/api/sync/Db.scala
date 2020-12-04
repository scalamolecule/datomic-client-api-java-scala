package datomicJava.client.api.sync

import java.util.stream.{Stream => jStream}
import java.util.{Date, List => jList, Map => jMap}
import datomicClient.{DbLookup, Invoke}
import datomicJava.client.api.Helper.streamOfDatoms
import datomicJava.client.api.{Datom, DbStats, Helper}


case class Db(
  datomicDb: AnyRef,
  sinceTimePoint: Option[(Long, Long, Date)] = None
) extends DbLookup(datomicDb, sinceTimePoint) {


  def dbStats: DbStats = {
    Helper.dbStats(
      isDevLocal,
      Invoke.dbStats(datomicDb).asInstanceOf[jMap[_, _]]
    )
  }


  // Time filters --------------------------------------

  def asOf(t: Long): Db = Db(Invoke.asOf(datomicDb, t))
  def asOf(d: Date): Db = Db(Invoke.asOf(datomicDb, d))

  def since(tOrTx: Long): Db =
    Db(Invoke.since(datomicDb, tOrTx), extractSinceTimePoint(tOrTx))

  def since(d: Date): Db =
    Db(Invoke.since(datomicDb, d), extractSinceTimePoint(d))


  // Presuming a `withDb` is passed.
  def `with`(withDb: AnyRef, stmts: jList[_]): TxReport =
    TxReport(Invoke.`with`(withDb, stmts).asInstanceOf[jMap[_, _]])

  // Convenience method to pass with-modified Db
  def `with`(db: Db, stmts: jList[_]): TxReport =
    `with`(db.datomicDb, stmts)

  // Convenience method to pass with-modified Db from TxReport
  def `with`(txReport: TxReport, stmts: jList[_]): TxReport =
    `with`(txReport.dbAfter.datomicDb, stmts)


  def history: Db = Db(Invoke.history(datomicDb))


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
  ): jStream[Datom] = {
    streamOfDatoms(
      Invoke.datoms(datomicDb, index, components, timeout, offset, limit)
    )
  }

  def datoms(index: String, components: jList[_], timeout: Int): jStream[Datom] =
    datoms(index, components, timeout, 0, 1000)

  def datoms(index: String, components: jList[_]): jStream[Datom] =
    datoms(index, components, 0, 0, 1000)


  def indexRange(
    attrId: String,
    start0: Any,
    end0: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): jStream[Datom] = {
    val start = Option(start0)
    val end = Option(end0)
    streamOfDatoms(
      Invoke.indexRange(
        datomicDb, attrId, start, end, timeout, offset, limit
      )
    )
  }

  def indexRange[T](
    attrId: String,
    start: Any,
    end: Any
  ): jStream[Datom] = indexRange(attrId, start, end, 0, 0, 1000)

  def indexRange[T](attrId: String, start: Any): jStream[Datom] =
    indexRange(attrId, start, None, 0, 0, 1000)

  def indexRange[T](attrId: String): jStream[Datom] =
    indexRange(attrId, None, None, 0, 0, 1000)


  // Pull --------------------------------------

  def pull(
    selector: String,
    eid: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): jMap[_, _] = {
    Invoke.pull(
      datomicDb, selector, eid, timeout, offset, limit
    ).asInstanceOf[jMap[_, _]]
  }

  def pull(selector: String, eid: Any, timeout: Int): jMap[_, _] =
    pull(selector, eid, timeout, 0, 1000)

  def pull(selector: String, eid: Any): jMap[_, _] =
    pull(selector, eid, 0, 0, 1000)


  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean,
    timeout: Int,
    offset: Int,
    limit: Int,
  ): jStream[_] = {
    Invoke.indexPull(
      datomicDb, index, selector, start, reverse, timeout, offset, limit
    ).asInstanceOf[clojure.lang.ASeq].stream()
  }

  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean,
    limit: Int,
  ): jStream[_] = {
    indexPull(index, selector, start, reverse, 0, 0, limit)
  }

  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean
  ): jStream[_] = {
    indexPull(index, selector, start, reverse, 0, 0, 1000)
  }

  def indexPull(
    index: String,
    selector: String,
    start: String
  ): jStream[_] = {
    indexPull(index, selector, start, false, 0, 0, 1000)
  }
}